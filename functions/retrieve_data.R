retrieve_data <- function(datasource = NULL, scientificname = NULL, taxonid = NULL, family = NULL,
    year = NULL, startyear = NULL, endyear = NULL, startdepth = NULL, enddepth = NULL,
    wkt = NULL, h3 = NULL, columns = NULL, return_query = FALSE) {

    require(DBI)
    require(duckdb)

    argg <- as.list(environment())
    argg <- argg[which(names(argg) != "return_query")]

    if (all(is.null(unlist(argg)))) stop("At least one argument should be passed.")

    if (sum(c(!is.null(scientificname), !is.null(family), !is.null(taxonid))) > 1) {
        stop("Only one of `scientificname`, `taxonid` or `family` should be supplied.")
    }

    if (!is.null(year)) {
        startyear <- endyear <- NULL
    }

    if (is.null(datasource)) {
        datasource <- "s3://obis-products/obistherm"
    } else {
        datasource <- sub("/+$", "", datasource)
    }

    if (is.null(columns)) {
        columns <- "*"
    } else if (columns[1] == "all") {
        columns <- "*"
    }

    if (!is.null(wkt) & !is.null(h3)) {
        stop("Only one of `wkt` or `h3` should be supplied.")
    } else if (!is.null(wkt)) {
        spatial_query <- glue::glue("ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))")
    } else if (!is.null(h3)) {
        h3 <- paste0("'", h3, "'", collapse = ", ")
        spatial_query <- glue::glue("h3_7 in ({h3});")
    } else {
        spatial_query <- ""
    }

    final_dq <- c()

    if (!is.null(year)) {
        year <- year[1]
        data_query <- glue::glue("year = {year}")
        final_dq <- c(final_dq, data_query)
    }
    if (!is.null(startyear) || !is.null(endyear)) {
        if (!is.null(startyear) & !is.null(endyear)) {
            years <- glue::glue(">= {startyear} and year <= {endyear}")
        } else if (!is.null(startyear)) {
            years <- glue::glue(">= {startyear}")
        } else if (!is.null(endyear)) {
            years <- glue::glue("<= {endyear}")
        }
        data_query <- glue::glue("year {years}")
        final_dq <- c(final_dq, data_query)
    }
    if (!is.null(scientificname)) {
        if (length(scientificname) > 1) {
            scientificname <- paste0("'", scientificname, "'", collapse = ", ")
            data_query <- glue::glue("species in ({scientificname})")
        } else {
            data_query <- glue::glue("species = '{scientificname}'")
        }
        final_dq <- c(final_dq, data_query)
    }
    if (!is.null(taxonid)) {
        if (length(taxonid) > 1) {
            taxonid <- paste0("'", taxonid, "'", collapse = ", ")
            data_query <- glue::glue("AphiaID in ({taxonid})")
        } else {
            data_query <- glue::glue("AphiaID = '{taxonid}'")
        }
        final_dq <- c(final_dq, data_query)
    }
    if (!is.null(family)) {
        if (length(family) > 1) {
            family <- paste0("'", family, "'", collapse = ", ")
            data_query <- glue::glue("family in ({family})")
        } else {
            data_query <- glue::glue("family = '{family}'")
        }
        final_dq <- c(final_dq, data_query)
    }
    if (!is.null(startdepth) || !is.null(enddepth)) {
        if (!is.null(startdepth) & !is.null(enddepth)) {
            depths <- glue::glue("minimumDepthInMeters >= {startdepth} and maximumDepthInMeters <= {enddepth}")
        } else if (!is.null(startdepth)) {
            depths <- glue::glue("minimumDepthInMeters >= {startdepth}")
        } else if (!is.null(enddepth)) {
            depths <- glue::glue("maximumDepthInMeters <= {enddepth}")
        }
        data_query <- depths
        final_dq <- c(final_dq, data_query)
    }

    if (spatial_query != "") {
        final_dq <- c(final_dq, spatial_query)
    }

    final_dq <- paste(final_dq, collapse = " and ")

    query <- glue::glue(
        "
select {columns}
  from read_parquet('{datasource}/*/*')
  where {final_dq}
        "
    )

    if (return_query) {
        cat(query, "\n\n")
        result <- query
    } else {
        con <- dbConnect(duckdb())
        dbSendQuery(con, "install httpfs; load httpfs;")
        if (!is.null(wkt)) {
            dbSendQuery(con, "install spatial; load spatial;")
        }

        result <- dbGetQuery(con, query)

        dbDisconnect(con)
    }

    return(result)

}
