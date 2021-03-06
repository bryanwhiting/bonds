% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{load_pqt}
\alias{load_pqt}
\title{Read in a multipart parquet file using a given file range}
\usage{
load_pqt(dir_root, tab, range = NULL)
}
\arguments{
\item{range}{}
}
\value{

}
\description{
Read in a multipart parquet file using a given file range
}
\examples{
read_parquet_table("files", range = 1:5)
}
