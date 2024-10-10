def generate_pyspark_code(fields, df_name):
    current_alias = fields.iloc[0]['Field'].split('.')[0]  # Start from the base field
    level_stack = [current_alias]  # To keep track of hierarchy levels
      # To store all the _pos fields for use in selections

    # PySpark code block as string
    code_str = f'df_{df_name} = df.select(col("_id").alias("root_id"),"*")\n'
    pos_fields = ["root_id"]
    for _, field in fields.iterrows():
        field_name = field["Field"]
        data_type = field["Data Type"]
        levels = field_name.split('.')  # Get hierarchy levels
        last_level = levels[-1]  # Get the last part of the field name

        # Handle the hierarchy
        while len(level_stack) > 1 and level_stack[-1] != levels[-2]:
            level_stack.pop()

        # Use *pos_fields to unpack the list into separate arguments for the select() method
        if data_type.startswith("array<struct>"):
            alias_name = f"new_{last_level}"
            # Add the pos field to the list
            if pos_fields:
                # Unpack pos_fields when calling select()
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, posexplode("{last_level}").alias("{last_level}_pos", "{alias_name}"))\n'
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, "{last_level}_pos", "{alias_name}.*")\n'
            else:
                code_str += f'df_{df_name} = df_{df_name}.select(posexplode("{last_level}").alias("{last_level}_pos", "{alias_name}"))\n'
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}_pos", "{alias_name}.*")\n'
            
            pos_fields.append(f"{last_level}_pos")  # Store pos field
            current_alias = alias_name
            level_stack.append(last_level)

        elif data_type.startswith("struct"):
            # For struct fields, select the current struct with existing _pos fields
            if pos_fields:
                # Unpack pos_fields when calling select()
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, "{last_level}.*")\n'
            else:
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}.*")\n'
            current_alias = last_level
            level_stack.append(last_level)

    # Call expand_structs to dynamically expand any struct fields present
    code_str += f'df_{df_name} = expand_structs(df_{df_name})\n'
    # Drop the exploded columns and aliases in a single line
    code_str += f'df_{df_name} = drop_child_arrays(df_{df_name})\n'

    code_str += f'df_{df_name}.printSchema()\n'
    code_str += f'df_{df_name}.toPandas().to_csv("{df_name}.csv", index=False)\n'

    return code_str
