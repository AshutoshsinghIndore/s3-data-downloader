import os
import pandas as pd
class file_utils:
    def get_file_list(path, ext='*', return_df=None):
        
                file_list = list()
                for root, dirs, files in os.walk(path):
                    for file in files:
                        file_list = file_list.__add__([os.path.join(root, file)])
        
                if ext == '*':
                    file_list = tuple(file_list)
                else:
                    file_list = tuple(
                        filter(lambda x: os.path.splitext(x)[-1] in ext, file_list))
        
                if return_df == None:
                    file_list = list(filter(lambda x: os.path.isfile(x), file_list))
                    return file_list
                else:
                    file_list = list(filter(lambda x: os.path.isfile(x), file_list))
                    df = pd.DataFrame({'Path': file_list,
                                       'FileName': [os.path.basename(path) for path in file_list],
                                       'FileSize (in bytes)': [os.path.getsize(path) for path in file_list]})
        
                    return df

def save_sync_state(df: pd.DataFrame, output_dir: str, file_name: str = 'sync_state.parquet') -> None:
    """
    Save the current sync state of S3 objects to a Parquet file.
    
    Args:
        df (pd.DataFrame): DataFrame containing object metadata (must include 'Key', 'LastModified', etc.)
        output_dir (str): Path where the Parquet file should be saved.
        file_name (str): Name of the Parquet file. Default is 'sync_state.parquet'.

    Raises:
        ValueError: If required columns are missing from the input DataFrame.
    """
    required_columns = {'Key', 'LastModified'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns in DataFrame: {required_columns - set(df.columns)}")

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, file_name)

    # Saving only necessary sync tracking columns (can be adjusted)
    df_to_save = df[['Key', 'LastModified', 'Size']].drop_duplicates()
    df_to_save.to_parquet(file_path, index=False)

    print(f"📦 Sync state saved at: {file_path}")