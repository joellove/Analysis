library("RODBC")

executeSql <- function(sql) {
    conn <- odbcConnect("AB_ODBC")
    
    #     if (!isSync) {    
    #         assign("isSync", TRUE, envir = .GlobalEnv)        
    #         
    splited <- strsplit(sql, split = " ")    
    tableName <- NULL
    for (i in 1:length(splited[[1]])) {
        if (splited[[1]][i] == "from") {
            tableName <- splited[[1]][i+1]
            break
        }
    }
    
    # META DATA INVALIDATE
    metaResult <- paste("INVALIDATE METADATA", tableName)        
    sqlQuery(conn, metaResult)
    
    #         isSync <- TRUE        
    #     } else {
    #         
    #     }
    
    result <- sqlQuery(conn, sql)
    odbcClose(conn)
    return (result)
}
