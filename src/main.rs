use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::{Context, FieldResult, Schema};
use async_std::sync::channel;
use async_std::task;
use futures::Stream;
use kv::*;
use std::env;

use tide::{http::mime, Body, Request, Response, StatusCode};
use uuid::Uuid;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
use serde::*;

#[async_graphql::SimpleObject]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    id: Uuid,
    description: String,
    amount: String,
}

#[async_graphql::InputObject]
#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionInput {
    description: String,
    amount: String,
}

struct QueryRoot;
struct MutationRoot;

#[async_graphql::Object]
impl QueryRoot {
    async fn transactions(&self, ctx: &Context<'_>) -> FieldResult<Vec<Transaction>> {
        // Using a Json encoded type is easy, thanks to Serde
        let bucket = ctx.data::<Bucket<&str, Json<Transaction>>>().unwrap();
        let items = bucket
            .iter()
            .map(|item| {
                let item = item.unwrap();
                let value = item.value::<Json<Transaction>>().unwrap();
                value.0
            })
            .collect::<Vec<_>>();

        Ok(items)
    }
}

#[async_graphql::Object]
impl MutationRoot {
    async fn create_transaction(
        &self,
        ctx: &Context<'_>,
        input: TransactionInput,
    ) -> FieldResult<Transaction> {
        // Using a Json encoded type is easy, thanks to Serde
        let bucket = ctx.data::<Bucket<&str, Json<Transaction>>>().unwrap();
        let uuid = Uuid::new_v4();
        let x = Transaction {
            id: uuid,
            amount: input.amount,
            description: input.description,
        };

        bucket.set(uuid.to_string().as_str(), Json(x.clone()))?;
        Ok(x)
    }
}

struct SubscriptionRoot;

#[async_graphql::Subscription]
impl SubscriptionRoot {
    async fn transactions(&self, ctx: &Context<'_>) -> impl Stream<Item = Transaction> {
        let (s, r) = channel(10_000_000);
        let bucket = ctx
            .data::<Bucket<&str, Json<Transaction>>>()
            .unwrap()
            .clone();

        task::spawn(async move {
            let handle = bucket.watch_prefix("").unwrap();
            for item in handle {
                dbg!("item");
                if let kv::Event::Set(i) = item.unwrap() {
                    let i = i.value::<Json<Transaction>>().unwrap();
                    s.send(i.0).await
                }
            }
        });

        r
    }
}

#[derive(Clone)]
struct AppState {
    schema: Schema<QueryRoot, MutationRoot, SubscriptionRoot>,
}

fn main() -> Result<()> {
    task::block_on(run())
}

async fn run() -> Result<()> {
    // Configure the database
    let cfg = Config::new("./database");

    // Open the key/value store
    let store = Store::new(cfg)?;
    let txns = store.bucket::<&str, Json<Transaction>>(None)?;

    let schema = Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(txns)
        .finish();

    let app_state = AppState { schema };
    let mut app = tide::with_state(app_state);

    async fn graphql(req: Request<AppState>) -> tide::Result<Response> {
        let schema = req.state().schema.clone();
        async_graphql_tide::graphql(req, schema, |query_builder| query_builder).await
    }

    app.at("/graphql").post(graphql).get(graphql);
    app.at("/").get(|_| async move {
        let mut resp = Response::new(StatusCode::Ok);
        resp.set_body(Body::from_string(playground_source(
            GraphQLPlaygroundConfig::new("/graphql"),
        )));
        resp.set_content_type(mime::HTML);
        Ok(resp)
    });

    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "localhost:8000".to_owned());
    println!("Playground: http://{}", listen_addr);
    app.listen(listen_addr).await?;

    Ok(())
}
