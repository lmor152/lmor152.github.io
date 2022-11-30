---
layout: default
title: Parquet From ADLS
parent: Power BI
nav_order: 1
---

This page describes approached to loading Parquet and Delta Lake tables from ADLS Gen2

# Parquet

```
(
    table_name as text
) as table =>

    let
        Source = AzureStorage.DataLake("https://aueprddlsnzlh001.dfs.core.windows.net/tp-bbc-processing/tables/tp_bbc_processing.db/" & table_name),
        #"Filtered Rows" = Table.SelectRows(Source, each ([Extension] = ".parquet")),
        #"Filtered Rows Sample" = Table.FirstN(#"Filtered Rows", 1),
        #"Parsed Parquet Sample" = Table.AddColumn(#"Filtered Rows Sample","Data", each Parquet.Document([Content])),
        #"Sample" = #"Parsed Parquet Sample"{[Name=Table.First(#"Filtered Rows Sample")[Name]]}[Data],
        #"Schema" = Table.Schema(#"Sample"),
        #"Schema With Definition" = Table.AddColumn(Schema, "Definition", each [Name] &"="& [Kind]),
        #"Parsed Parquet" = Table.AddColumn(#"Filtered Rows","Data", each Parquet.Document([Content]), Expression.Evaluate("type table [" & Text.Combine(#"Schema With Definition"[Definition], ", ") & "]")), // type table [year=number]),
        // Can we optimise? https://blog.crossjoin.co.uk/2021/03/29/optimising-the-performance-of-combining-data-from-multiple-parquet-files-in-power-query-power-bi/
        #"Expanded Data" = Table.ExpandTableColumn(#"Parsed Parquet", "Data", List.Union(List.Transform(#"Parsed Parquet"[Data], each Table.ColumnNames(_)))),
        #"Selected Cols" = Table.SelectColumns(#"Expanded Data", Table.ColumnNames(#"Sample"))
    in
        #"Selected Cols"
```

# Delta Lake
https://blog.gbrueckl.at/2021/01/reading-delta-lake-tables-natively-in-powerbi/


# Selecting at runtime

```
(
    table_name as text,
    load_from_parquet as logical
) as table =>

    let
        Loaded = if load_from_parquet  then ReadFromParquet(table_name) else ReadFromDatabricks(table_name)
    in
        Loaded
```

```
(
    table_name as text
) as table =>

    let
        Source = AzureStorage.DataLake("https://aueprddlsnzlh001.dfs.core.windows.net/tp-bbc-processing/tables/tp_bbc_processing.db/" & table_name),
        #"Filtered Rows Sample" = Table.SelectRows(Source, each ([Extension] = ".parquet")),
        #"Parsed Parquet Sample" = Table.AddColumn(#"Filtered Rows Sample","Data", each Parquet.Document([Content])),
        #"Sample" = #"Parsed Parquet Sample"{[Name=Table.First(#"Filtered Rows Sample")[Name]]}[Data],
        #"Schema" = Table.Schema(#"Sample"),
        #"Schema With Definition" = Table.AddColumn(Schema, "Definition", each [Name] &"="& [Kind]),
        #"Filtered Rows" = Table.SelectRows(Source, each ([Extension] = ".parquet")),
        #"Parsed Parquet" = Table.AddColumn(#"Filtered Rows","Data", each Parquet.Document([Content]), Expression.Evaluate("type table [" & Text.Combine(#"Schema With Definition"[Definition], ", ") & "]")), // type table [year=number]),
        // Can we optimise? https://blog.crossjoin.co.uk/2021/03/29/optimising-the-performance-of-combining-data-from-multiple-parquet-files-in-power-query-power-bi/
        #"Expanded Data" = Table.ExpandTableColumn(#"Parsed Parquet", "Data", List.Union(List.Transform(#"Parsed Parquet"[Data], each Table.ColumnNames(_)))),
        #"Selected Cols" = Table.SelectColumns(#"Expanded Data", Table.ColumnNames(#"Sample"))
    in
        #"Selected Cols"
```

```
(
    table_name as text
) as table =>

    let
        Source = Databricks.Contents("adb-5195694350474952.12.azuredatabricks.net", "sql/protocolv1/o/5195694350474952/0314-215000-cocoa527", [Database=null, BatchSize=500000]),
        #"Spark Database" = Source{[Name="SPARK",Kind="Database"]}[Data],
        #"Spark Schema" = #"Spark Database"{[Name="tp_bbc_processing",Kind="Schema"]}[Data],
        #"Spark Table" = #"Spark Schema" {[Name=table_name,Kind="Table"]}[Data]
    in
        #"Spark Table"
```