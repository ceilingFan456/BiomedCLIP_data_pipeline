MAX_ITEMS_TO_PROCESS = 100

from pmc15_pipeline import data
from pmc15_pipeline.utils import fs_utils

repo_root = fs_utils.get_repo_root_path()

list_output_path = repo_root / "_results" / "data" / "pubmed_open_access_file_list.txt"

data.download_pubmed_file_list(
    output_file_path=list_output_path,
)

# downloaded_articles_output_path = repo_root / "_results" / "data" / "pubmed_open_access_files_compressed"

# data.download_pubmed_files_from_list(
#     file_list_path=list_output_path,
#     output_folder_path=downloaded_articles_output_path,
#     subset_size=MAX_ITEMS_TO_PROCESS,
# )

# decompressed_folder_path = repo_root / "_results" / "data" / "pubmed_open_access_files"

# data.decompress_pubmed_files(
#     input_folder_path=downloaded_articles_output_path,
#     output_folder_path=decompressed_folder_path,
# )

# pipeline_input_file_path = repo_root / "_results" / "data" / "pubmed_parsed_data.jsonl"

# data.generate_pmc15_pipeline_outputs(
#     decompressed_folder=decompressed_folder_path,
#     output_file_path=pipeline_input_file_path,
# )

# num_lines = fs_utils.get_line_count(pipeline_input_file_path)
# print(f"Number of lines in pipeline output file: {num_lines}")