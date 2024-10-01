get_samples <- function(.usr, .pwd) {

    fs::dir_create("samples")

    if (!file.exists("samples/glorys.nc")) {
        outf <- cm$get(
            dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1M-m",
            # variables = list("thetao"),
            username = .user,
            password = .pwd,
            filter = "*202101*",
            output_directory = "samples/",
            no_directories = T,
            force_download = T
        )
        glorys_sample <- rast(as.character(outf[[1]]))
        glorys_sample <- subset(glorys_sample, startsWith(names(glorys_sample), "thetao"))
        origin(glorys_sample)[1] <- 0
        origin(glorys_sample)[2] <- origin(glorys_sample)[2] * 2
        writeRaster(glorys_sample, "samples/glorys_ed.nc")
        file.remove(as.character(outf[[1]]))
    }

    if (!file.exists("samples/coraltemp_ed.nc")) {
        coraltemp_sample <- xr$open_dataset("https://coastwatch.pfeg.noaa.gov/erddap/griddap/NOAA_DHW_monthly")
        coraltemp_sample <- coraltemp_sample$sea_surface_temperature
        coraltemp_sample <- coraltemp_sample$sel(time = paste0("2021-", 1:12, "-16"), method = "nearest")
        coraltemp_sample$to_netcdf("samples/coraltemp.nc")
        ct_r <- rast("samples/coraltemp.nc")
        ct_r <- mean(ct_r, na.rm = T)
        ct_r <- flip(ct_r)
        writeRaster(ct_r, "samples/coraltemp_ed.nc")
        fs::file_delete("samples/coraltemp.nc")
        rm(coraltemp_sample, ct_r)
    }

    if (!file.exists("samples/mur_ed.tif")) {
        download.file("https://coastwatch.pfeg.noaa.gov/erddap/files/jplMURSST41mday/2021060120210630-GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc",
            "samples/mur.nc",
            method = "wget"
        )
        mur <- rast("samples/mur.nc")
        mur <- mur[[1]]
        writeRaster(mur, "samples/mur_ed.tif")
        fs::file_delete("samples/mur.nc")
        rm(mur)
    }

    if (!file.exists("samples/ostia.nc")) {
        ostia_sample <- cm$open_dataset(
            dataset_id = "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
            variables = list("analysed_sst"),
            username = .user,
            password = .pwd
        )
        ostia_sample <- ostia_sample$sel(time = "2021-06-01", method = "nearest")
        ostia_sample$to_netcdf("samples/ostia.nc")
        rm(ostia_sample)
    }

    cat("All samples downloaded and available in the folder `samples`")

    return(invisible(NULL))
}