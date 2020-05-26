use bytes::{Buf, Bytes};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use md5::Context as Md5;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use sha2::{Digest, Sha384};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWriteExt},
    task,
};
use tokio_util::codec;
use warp::{http::StatusCode, Filter};

use std::convert::Infallible;
use std::env;

#[derive(Debug)]
enum PutFileError {
    InvalidFileType,
}

impl warp::reject::Reject for PutFileError {}

struct FileType {
    ext: &'static str,
    mime: &'static str,
}

async fn tempfile() -> std::io::Result<File> {
    task::spawn_blocking(|| tempfile::tempfile().map(File::from_std))
        .await
        .unwrap()
}

fn into_bytes_stream<R>(r: R) -> impl Stream<Item = tokio::io::Result<Bytes>>
where
    R: AsyncRead,
{
    codec::FramedRead::new(r, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
}

async fn put_file(
    mut body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + Send + Sync + 'static,
    s3_bucket: String,
    s3_client: S3Client,
) -> Result<impl warp::Reply, warp::Rejection> {
    let first_chunk = body.next().await.unwrap().unwrap();
    let first_chunk = first_chunk.bytes();

    let file_type = match first_chunk {
        [0xFF, 0xD8, 0xFF, ..] => FileType {
            ext: "jpg",
            mime: "image/jpeg",
        },
        [0x47, 0x49, 0x46, ..] => FileType {
            ext: "gif",
            mime: "image/gif",
        },
        [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, ..] => FileType {
            ext: "png",
            mime: "image/png",
        },
        _ => return Err(warp::reject::custom(PutFileError::InvalidFileType)),
    };

    let mut md5 = Md5::new();
    let mut sha384 = Sha384::new();
    let mut file = tempfile().await.unwrap();
    let mut file_size = 0;

    md5.consume(first_chunk);
    sha384.input(first_chunk);
    file.write_all(first_chunk).await.unwrap();
    file_size += first_chunk.len();

    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap();
        let chunk = chunk.bytes();

        md5.consume(chunk);
        sha384.input(chunk);
        file.write_all(chunk).await.unwrap();
        file_size += chunk.len();
    }

    let content_md5 = base64::encode_config(*md5.compute(), base64::STANDARD);
    let content_hash = base64::encode_config(sha384.result(), base64::URL_SAFE_NO_PAD);
    let key = format!("{}/original.{}", content_hash, file_type.ext);

    file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let file = into_bytes_stream(file);

    let req = PutObjectRequest {
        acl: Some("public-read".to_owned()),
        body: Some(ByteStream::new(file)),
        bucket: s3_bucket.clone(),
        cache_control: Some("public,max-age=31536000,immutable".to_owned()),
        content_length: Some(file_size as i64),
        content_md5: Some(content_md5),
        content_type: Some(file_type.mime.to_owned()),
        key,
        ..Default::default()
    };

    s3_client.put_object(req).await.unwrap();

    Ok(format!(
        "https://{}/{}/original.{}",
        s3_bucket, content_hash, file_type.ext
    ))
}

async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    match err.find::<PutFileError>() {
        Some(PutFileError::InvalidFileType) => Ok(warp::reply::with_status(
            "Invalid file type",
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(warp::reply::with_status(
            "Internal server error",
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

#[tokio::main]
async fn main() {
    let s3_bucket = env::var("S3_BUCKET").expect("Missing S3_BUCKET in environemnt");
    let with_s3_bucket = warp::any().map(move || s3_bucket.clone());

    let s3_client = S3Client::new(Region::EuNorth1);
    let with_s3_client = warp::any().map(move || s3_client.clone());

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE"])
        .build();

    let handler = warp::put()
        .and(warp::path!("v1" / "files"))
        .and(warp::filters::body::stream())
        .and(with_s3_bucket)
        .and(with_s3_client)
        .and_then(put_file)
        .with(cors.clone())
        .recover(handle_rejection);

    let preflight = warp::options()
        .map(warp::reply)
        .with(cors);

    warp::serve(preflight.or(handler)).run(([0, 0, 0, 0], 25635)).await;
}
