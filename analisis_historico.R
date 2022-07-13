library(tidyverse)
library(janitor)

#Descargamos la base
db <- read_csv("Datos_Historicos.csv",
               col_types = cols(
                 .default       = col_integer(),
                 FECHA_SINTOMAS = col_date(format = "%Y-%m-%d")
               )) %>%
  arrange(FECHA_SINTOMAS)


#Obtenemos las columnas que no son fecha o entidad
viable_cols <- str_which(colnames(db),"FECHA|ENTIDAD", negate = TRUE)

#Acomodamos las columnas
dbdates <- as.Date(colnames(db)[viable_cols], format = "%Y-%m-%d")
db      <- db %>%
  select(matches("FECHA|ENTIDAD"), as.character(sort(dbdates)))


