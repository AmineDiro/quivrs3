use futures::stream::FuturesUnordered;
use futures::StreamExt;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use reqwest::header::CONTENT_LENGTH;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::codec::{BytesCodec, FramedRead};

const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;

#[inline(always)]
pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
    (base_wait_time + n.pow(2)).min(max)
}
async fn upload_async(
    file_path: String,
    parts_urls: Vec<String>,
    chunk_size: u64,
    max_files: usize,
    parallel_failures: usize,
    max_retries: usize,
) -> PyResult<Vec<HashMap<String, String>>> {
    let client = reqwest::Client::new();

    let mut handles = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(max_files));
    let parallel_failures_semaphore = Arc::new(Semaphore::new(parallel_failures));

    for (part_number, part_url) in parts_urls.iter().enumerate() {
        let url = part_url.clone();
        let path = file_path.clone();
        let client = client.clone();

        let start = (part_number as u64) * chunk_size;
        let semaphore = semaphore.clone();
        let parallel_failures_semaphore = parallel_failures_semaphore.clone();
        handles.push(tokio::spawn(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|err| PyException::new_err(format!("Error acquiring semaphore: {err}")))?;
                    let mut chunk = upload_chunk(&client, &url, &path, start, chunk_size).await;
                    let mut i = 0;
                    if parallel_failures > 0 {
                        while let Err(ul_err) = chunk {
                            if i >= max_retries {
                                return Err(PyException::new_err(format!(
                                    "Failed after too many retries ({max_retries:?}): {ul_err:?}"
                                )));
                            }

                            let parallel_failure_permit = parallel_failures_semaphore.clone().try_acquire_owned().map_err(|err| {
                                PyException::new_err(format!(
                                    "Failed too many failures in parallel ({parallel_failures:?}): {ul_err:?} ({err:?})"
                                ))
                            })?;

                            let wait_time = exponential_backoff(BASE_WAIT_TIME, i, MAX_WAIT_TIME);
                            sleep(tokio::time::Duration::from_millis(wait_time as u64)).await;

                            chunk = upload_chunk(&client, &url, &path, start, chunk_size).await;
                            i += 1;
                            drop(parallel_failure_permit);
                        }
                    }
                    drop(permit);
                    chunk.and_then(|chunk| Ok((part_number, chunk, chunk_size)))
                }));
    }

    let mut results: Vec<HashMap<String, String>> = vec![HashMap::default(); parts_urls.len()];

    // let results: Result<Vec<usize>, _> = join_all(tasks).await.into_iter().collect();
    // results.map_err(|e| PyException::new_err(format!("an error occured:  {:?} ", e)))

    while let Some(result) = handles.next().await {
        match result {
            Ok(Ok((part_number, headers, _size))) => {
                results[part_number] = headers;
            }
            Ok(Err(py_err)) => {
                return Err(py_err);
            }
            Err(err) => {
                return Err(PyException::new_err(format!(
                    "Error occurred while uploading: {err}"
                )));
            }
        }
    }

    Ok(results)
}

async fn upload_chunk(
    client: &reqwest::Client,
    url: &str,
    path: &str,
    start: u64,
    chunk_size: u64,
) -> PyResult<HashMap<String, String>> {
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;
    let file_size = file.metadata().await?.len();
    let bytes_transfered = std::cmp::min(file_size - start, chunk_size);

    file.seek(SeekFrom::Start(start)).await?;
    let chunk = file.take(chunk_size);

    let response = client
        .put(url)
        .header(CONTENT_LENGTH, bytes_transfered)
        .body(reqwest::Body::wrap_stream(FramedRead::new(
            chunk,
            BytesCodec::new(),
        )))
        .send()
        .await
        .map_err(|err| PyException::new_err(format!("Error sending chunk: {err}")))?
        .error_for_status()
        .map_err(|err| {
            PyException::new_err(format!(
                "Server responded with error status code while upload chunk: {err}"
            ))
        })?;

    let mut headers = HashMap::new();
    for (name, value) in response.headers().into_iter() {
        headers.insert(
            name.to_string(),
            value
                .to_str()
                .map_err(|err| {
                    PyException::new_err(format!("Response header contains non ASCII chars: {err}"))
                })?
                .to_owned(),
        );
    }
    Ok(headers)
}

#[pyfunction]
fn multipart_upload(
    py: Python,
    file_path: String,
    parts_urls: Vec<String>,
    chunk_size: u64,
    max_files: usize,
    parallel_failures: usize,
    max_retries: usize,
) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        upload_async(
            file_path,
            parts_urls,
            chunk_size,
            max_files,
            parallel_failures,
            max_retries,
        )
        .await
    })
}

#[pymodule]
fn upload(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multipart_upload, m)?)?;
    Ok(())
}
