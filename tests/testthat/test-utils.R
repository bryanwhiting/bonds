test_that("extract_filenames() works", {
  expect_equal(extract_file_idx(), 1:15)
})

test_that("create_filename() works", {
  ans <-  "/home/rstudio/data/data-challenge/data/interconnect_json/file_3.json"
  obs <- as.character(create_filename(idx=3))
  expect_equal(obs, ans)
})

test_that("read_parquet_table() works", {
  x <- read_parquet_table('files', range=4:6)
  expect_true(min(x$file_id) == 4)
  expect_true(max(x$file_id) == 6)
})
