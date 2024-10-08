# Recursive function to expand nested structs until there are no more struct columns
def expand_structs(df):
    from pyspark.sql.types import StructType
    
    while True:
        struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
        
        if not struct_columns:
            break
        
        # Expand the first-level struct columns
        for struct_col in struct_columns:
            df = df.select("*", f"{struct_col}.*").drop(struct_col)
    
    return df
