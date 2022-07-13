rm(list = ls())

library(glue)
library(lubridate)
library(tidyverse)
library(DBI)

zip_files     <- "/media/rodrigo/covid/datasets/" #Directory where the zip files are
unzip_command <- "unzip" #Set the path to `where 7zip`
unzip_args    <- "-o" #-x on windows
tablename     <- "covidmxhistoric"
nthreads      <- 2
user          <- Sys.getenv("MariaDB_user")
password      <- Sys.getenv("MariaDB_password")

bases_datos <- list.files(zip_files, pattern = "datos_abiertos_covid19_.*zip",
                          full.names = TRUE)

#Leemos la info que ya tenemos de históricos
historicos  <- read_csv("Datos_Historicos.csv")

fechas_df   <- colnames(historicos)[which(!(colnames(historicos) %in% c("FECHA_SINTOMAS","ENTIDAD_UM")))]

for (fecha in as.list(ymd(fechas_df))){
  dia <- as.character(day(fecha))
  dia <- ifelse(nchar(dia) == 1, paste0("0",dia),dia)
  mes <- as.character(month(fecha))
  mes <- ifelse(nchar(mes) == 1, paste0("0",mes),mes)
  pos <- str_which(basename(bases_datos), 
                   glue("datos_abiertos_covid19_{dia}.{mes}.{year(fecha)}.zip"))
  if (length(pos) > 0){
    bases_datos <- bases_datos[-pos]
  }
}

#Leemos la base histórica, subimos a MARIADB y tibbleamos
for (db in bases_datos){
  
  message(glue("Unzipping {db}"))
  fdownload <- tempfile()
  
  suppressMessages(
    system2(unzip_command, args = c(unzip_args, db))
  )
  
  filecon <- list.files(pattern = "*COVID19MEXICO.csv", full.names = T)[1]
  
  #Create table if not exists
  con <- DBI::dbConnect(RMariaDB::MariaDB(),
                        user     = user,
                        password =  Sys.getenv("MariaDB_password"),
                        port     =  Sys.getenv("MariaDB_port"),
                        dbname   = "covidmx",
                        tblname  = tablename)
  
  #Lectura del header
  header <- readr::read_csv(filecon, locale = readr::locale(encoding = "UTF-8"),
                            n_max = 1,
                            trim_ws = TRUE,
                            col_types = readr::cols(
                              .default            = col_skip(),
                              #ID_REGISTRO        = col_character(),
                              ENTIDAD_UM          = col_double(),
                              #TIPO_PACIENTE       = col_character(),
                              #FECHA_ACTUALIZACION = col_date(format = "%Y-%m-%d"),
                              #FECHA_INGRESO       = col_date(format = "%Y-%m-%d"),
                              FECHA_SINTOMAS      = col_date(format = "%Y-%m-%d"),
                              #FECHA_DEF           = col_date(format = "%Y-%m-%d"),
                              RESULTADO_LAB       = col_double(),
                              #CLASIFICACION_FINAL = col_character(),
                              RESULTADO           = col_double(),
                              RESULTADO_ANTIGENO  = col_double(),
                            ))
  
  #Lectura del todo para comparar con el header
  sqlposp <- readr::read_csv(filecon, locale = readr::locale(encoding = "UTF-8"),
                            n_max = 1,
                            trim_ws = TRUE,
                            col_types = readr::cols(
                              .default            = col_character(),
                              ENTIDAD_UM          = col_double(),
                              FECHA_SINTOMAS      = col_date(format = "%Y-%m-%d"),
                              RESULTADO_LAB       = col_double(),
                              RESULTADO           = col_double(),
                              RESULTADO_ANTIGENO  = col_double(),
                            )) 
  
  #SI la tabla no existe la creamos
  dbres <- DBI::dbSendStatement(con, "SET sql_mode = 'NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER';")
  dbClearResult(dbres)
    
  #Write table
  DBI::dbWriteTable(con, tablename, value = header, overwrite = T)
    
  #Borramos a tabla si ya existe 
  dbres <- DBI::dbSendStatement(con, glue("DELETE FROM {tablename}"))
  dbClearResult(dbres)    
    
  #Lectura de fecha de actualización sale más eficiente agregar al final
  fecha_act <- readr::read_csv(filecon, locale = readr::locale(encoding = "UTF-8"), n_max = 1, 
                               trim_ws = TRUE,
                               col_types = 
                                 cols(.default = col_skip(),
                                      FECHA_ACTUALIZACION = col_date(format = "%Y-%m-%d")))
  
  if (file.exists(glue("./{tablename}.csv"))){
    file.remove(glue("./{tablename}.csv"))
  }
  file.rename(filecon, glue("./{tablename}.csv"))
  
  #Generamos la tabla a partir del csv
  col_positions                                 <- colnames(sqlposp) %in% colnames(header)
  col_positions[!col_positions]                 <- "@ignore"
  col_positions[which(col_positions == "TRUE")] <- colnames(header)
  columns_sql <- paste0(col_positions, collapse = ",")
  
  suppressMessages(
    system(glue::glue("mysqlimport --default-character-set=UTF8",
                      " --fields-terminated-by=','",
                      " --ignore-lines=1",
                      " --fields-enclosed-by='\"'",
                      " --lines-terminated-by='\\n'",
                      " --user={user}",
                      " --password={password}",
                      " --use-threads={nthreads}",
                      " --columns={columns_sql}",
                      " --local covidmx ./{tablename}.csv"))
  )
  
  #Leemos la conexión
  dats <- dplyr::tbl(con, tablename)
  
  #Creamos el tibble
  #Antes del 28 de noviembre 2020 se incluía la variable resultado = 1 si covid 
  #a partir de otro día se incluye CLASIFICACION_FINAL = 1,2 ó 3 para covid
  #y en algún momento intermedio hubo otra CLASIFICACION_FINAL
  
  tibble_report <- dats %>%
    filter(if_any(starts_with("RESULTADO"), ~ (.x == 1))) %>%
    group_by(FECHA_SINTOMAS, ENTIDAD_UM) %>%
    tally() %>%
    ungroup() %>%
    collect() %>%
    mutate(FECHA_ACTUALIZACION = as.vector(!!fecha_act)[[1]]) %>% 
    mutate(n = as.double(n)) %>%
    pivot_wider(id_cols = c(FECHA_SINTOMAS, ENTIDAD_UM), 
                names_from = FECHA_ACTUALIZACION, values_from = n) 
  
  historicos <- tibble_report %>%
    full_join(historicos) %>%
    mutate_if(is.numeric, coalesce, 0)
  
  write_excel_csv(historicos, "Datos_Historicos.csv")
  
  if (file.exists(filecon)){
    file.remove(filecon)
  }
  
  dbDisconnect(con)
}
