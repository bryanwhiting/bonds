#' @export
dir_output <- here::here("output")

#' @export
db_path <- here::here(dir_output, "data.db")

#' @export
dir_pqt <- here::here(dir_output, "pqt")

#' @export
dir_pqt_all <- here::here(dir_pqt, "all")

#' @export
dir_pqt_grpid <- here::here(dir_pqt, "grpid")

#' Extracts integers from filenames
#'
#' @param dir directory where files are stored on nfs
#'
#' @return
#' @export
#'
#' @examples
extract_file_idx <- function(dir = "/home/rstudio/data/data-challenge/data/interconnect_json/") {
  dir_ls(dir) %>%
    basename() %>%
    str_replace("file_(.*)\\.json", "\\1") %>%
    as.numeric() %>%
    sort() %>%
    return()
}

create_filename <- function(idx, root = "/home/rstudio/data/data-challenge/data/interconnect_json/") {
  path(root, glue("file_{idx}"), ext = "json") %>%
    return()
}

load_file_as_tibble <- function(json_file) {
  jsonlite::fromJSON(json_file) %>%
    as_tibble() %>%
    janitor::clean_names() %>%
    return()
}

#' Parse and Clean Data
#'
#' The raw data is a nested dataframe. The command "unnest"
#' will take the arrays and flatten them, repeating each
#' row of the parent.
#'
#' I create three dataframes here, to make it easy to
#' save them out to SQL. I retain the file_id in the bonds table
#' and the bond_id in the sample table to make it easy to join.
#'
#' @param df
#'
#' @return
#' @export
#'
#' @examples
parse_and_clean_data <- function(df) {
  # Clean the raw data
  # unnest will repeat values for every row in array $bonds
  # selecting file_id enables me to retain a primary key

  df_bonds <- df %>%
    select(file_id, bonds) %>%
    unnest(bonds) %>%
    janitor::clean_names()

  # Create table of samples with file_id and bond_id for joins
  df_samples_final <- df_bonds %>%
    select(file_id, bond_id, trace_samples) %>%
    unnest(trace_samples) %>%
    janitor::clean_names()

  # create a bonds table by dropping the trace_samples
  df_bonds_final <- df_bonds %>%
    select(-trace_samples) %>%
    mutate(date_time = lubridate::ymd_hms(date_time))


  df_file_final <- df %>%
    select(-bonds) %>%
    distinct() %>%
    mutate(start_date_time = lubridate::ymd_hms(start_date_time))


  # save out a dataset ready for modeling by bond_group_id
  df_bonds_with_samples_final <- df_bonds %>%
    mutate(date_time = lubridate::ymd_hms(date_time)) %>%
    unnest(trace_samples)

  return(
    list(
      files = df_file_final,
      bonds = df_bonds_final,
      samples = df_samples_final,
      bonds_with_samples = df_bonds_with_samples_final
    )
  )
}

#' Loads the three data sets into a sqlite database
#'
#' @param data List of dataframes
#' @param db_path database path
#'
#' @return
#' @export
#'
#' @examples
load_to_sqlite <- function(data, db = db_path, idx = NULL) {
  # con <- dbConnect(RSQLite::SQLite(), db=db_path)
  pool <- dbPool(RSQLite::SQLite(), db = db)
  # Initialize the schema
  created_tables <- dbListTables(conn = pool)
  for (tab in c("files", "bonds", "samples")) {
    if (tab %in% created_tables) {
      add_column_if_doesnt_exist(df = data[[tab]], pool = pool, tab = tab)
      logger::log_info("Updating [data.", tab, "] from file {idx}")
      dbAppendTable(conn = pool, name = tab, data[[tab]])
    } else {
      dbWriteTable(conn = pool, name = tab, data[[tab]])
    }
  }
  # dbDisconnect(con)
  poolClose(pool)
}

#' Checks if every column in the dataset exists in the
#' SQLITE table. If it doesn't, it adds one.
#'
#' @param data List of dataframes
#' @param pool Connection
#' @param table
#'
#' @return
#' @export
#'
#' @examples
add_column_if_doesnt_exist <- function(df, pool, tab) {
  # debugging
  # df <- data[[tab]]
  # cols <- colnames(df)
  # tab = 'bonds'

  # List columns in sqlite table
  # https://stackoverflow.com/a/948204
  db_columns <- dbGetQuery(pool, glue("PRAGMA table_info({tab})")) %>%
    pull(name)

  for (c in colnames(df)) {
    if (!(c %in% db_columns)) {
      # What value should it be in SQLite?
      # dbDataType returns the sqlite value from the R object type
      db_data_type <- dbDataType(pool, df[[c]])
      logger::log_info("Adding ", c, " AS ", db_data_type, " to [data.", tab, "]")

      # Add new column
      # https://stackoverflow.com/a/50893477
      query <- glue("ALTER TABLE {tab} ADD COLUMN {c} {db_data_type};")
      dbExecute(pool, query)
    }
    # TODO: Ensure column was added
    # new_cols <- dbGetQuery(pool, glue('PRAGMA table_info({tab})')) %>%
    #   pull(name)
    # msg = glue('Column {c} was NOT added.') %>% as.character()
    # stopifnot(c %in% new_cols)
  }
}

#' Split data by bond_group_id
#'
#' splits bond sample data into partitions by
#' bond_group_id
#'
#' @param data list with a dataframe called bonds_with_samples inside
#'
#' @return
#' @export
#'
#' @examples
prep_group_id_data <- function(data){
  df_bws <- data$bonds_with_samples
  # how to split a df into lists: https://stackoverflow.com/a/18527515
  df_bws_split <- split(df_bws, f=df_bws$bond_group_id)
  return(df_bws_split)
}


#' Write as parquet
#'
#' Creates individual tables for each section of json object
#'
#' @param data
#' @param idx
#'
#' @return
#' @export
#'
#' @examples
save_as_parquet <- function(data, dir_root, idx) {
  tabs <- names(data)
  # Loop over tab names and save out as pqt/bonds/part-00001.parquet
  purrr::map(tabs, ~ {
    fp <- here::here(dir_root, ., glue("part-{str_pad(idx,5,pad=0)}.parquet"))
    fs::dir_create(path_dir(fp))
    arrow::write_parquet(data[[.]], fp)
  })
}

#' Run the entire ETL process for one file index
#'
#' Assumes files have format /././file_<idx>.json
#'
#' @param idx
#'
#' @return
#' @export
#'
#' @examples
etl_one_file <- function(idx) {
  # all steps (process all files)
  fp <- create_filename(idx = idx)

  logger::log_info('Parsing JSON')
  df <- load_file_as_tibble(fp)

  logger::log_info('Cleaning data')
  data <- parse_and_clean_data(df)

  logger::log_info('Loading to sql')
  tic()
  load_to_sqlite(data, idx = idx)
  toc()

  # save as all
  logger::log_info('Save as pqt all')
  tic()
  all <- data[c('files', 'bonds', 'samples')]
  save_as_parquet(data=all, dir_root=dir_pqt_all, idx = idx)
  toc()

  # split into parquet file by group_id
  logger::log_info('Save as pqt by group_id')
  tic()
  grpid <- data[c('bonds_with_samples')]
  ls_grpid <- prep_group_id_data(grpid)
  save_as_parquet(data = ls_grpid, dir_root=dir_pqt_grpid, idx = idx)
  toc()

  logger::log_info(glue('Finished file{idx}'))
}




#' Process all files in the given folder
#'
#' @return data.db file with three tables
#' @export
#'
#' @examples
etl_all_files <- function() {
  idxs <- extract_file_idx()
  for (i in idxs) {
    print(i)
    etl_one_file(i)
  }
  # purrr::map(idxs, etl_one_file)

  # TODO: parallelize
  # future::plan(future::multisession, workers = 3)
  # furrr::future_map(idxs,
  #                   ~etl_all_files,
  #                   .options = furrr::furrr_options(globals='etl_all_files') )
}

db_to_fst <- function(db_path = db_path) {
  pool <- dbPool(RSQLite::SQLite(), db = db_path)

  x <- dbGetQuery(pool, "
    select *
    from samples
    left join bonds using(bond_id)
    left join files using(file_id)
    where
    samples.file_id = 1
    AND files.file_id = 1
    AND bonds.file_id = 1
  ")
}

#' Read in multipart parquet
#' https://stackoverflow.com/a/58450079
#'
#' @param file_list
#'
#' @return
#' @export
glob_parquets <- function(file_list){
  df <- data.table::rbindlist(
    lapply(
      file_list,
      function(x) arrow::read_parquet(x)
    ),
    fill = TRUE
  )
  return(df)
}

#' Read in a multipart parquet file using a given file range
#'
#' @param tab
#' @param range
#'
#' @return
#' @export
#'
#' @examples
#' read_parquet_table("files", range = 1:5)
load_pqt <- function(dir_root, tab, range = NULL) {
  d <- here::here(dir_root, tab)
  file_list <- dir_ls(d)
  if (!is.null(range)) {
    part_files <- str_pad(range, 5, pad = "0")
    file_list <- glue("{d}/part-{part_files}.parquet")
  }

  df <- glob_parquets(file_list)
  return(as_tibble(df))
}

# TODO: create automl tool?
# prep_for_ml <- function(db_path = db_path) {
#   pool <- dbPool(RSQLite::SQLite(), db = db_path)
#   dbGetQuery(
#     pool,
#     "select"
#   )
# }
