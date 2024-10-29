from io import BytesIO
from typing import Union

import pandas as pd

def xlsx_sheets_io(sheets_to_write: dict[str, pd.DataFrame], buffer: Union[str, BytesIO]):
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        for sheet_name, df in sheets_to_write.items():
            # Write data to Excel without the index
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Access the workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            # Create a format for word wrapping
            wrap_format = workbook.add_format({'text_wrap': True})

            # Auto-size columns, but limit the size to a maximum of 100
            for i, col in enumerate(df.columns):
                # Handle NaN or missing values by replacing them with empty strings
                col_data = df[col].astype(str).fillna('')

                # Calculate the maximum length between the header and the data, avoiding ambiguous Series
                max_len = max(col_data.apply(lambda x: len(str(x))).max(), len(col)) + 2  # Add extra padding
                max_len = min(max_len, 70)  # Limit column width to 100

                # Set the column width, and apply word wrap format if the max size is hit
                if max_len >= 100:
                    worksheet.set_column(i, i, max_len, wrap_format)  # Enable word wrap
                else:
                    worksheet.set_column(i, i, max_len)

            # Define the range of the table
            (max_row, max_col) = df.shape

            # Add the table with auto-filter enabled
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': [{'header': column} for column in df.columns],
                'autofilter': True
            })