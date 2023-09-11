import argparse
import webbrowser
from datetime import datetime

import  sfbulk2  as gl

# Example
# python example_gl.py --git_folder  <git_folder>  --from_date 2023-08-01 --output_csv ~/git-functions/commit_logs.csv
def main():
    parser = argparse.ArgumentParser(description='List Git commit logs for a given folder')
    parser.add_argument('--git_folder', type=str, help='Path to the Git repository folder')
    parser.add_argument('--from_date', type=str, help='Filter commits from the specified date (YYYY-MM-DD)')
    parser.add_argument('--output_csv', type=str, help='Specify the output CSV file')
    args = parser.parse_args()

    git_folder = args.git_folder
    from_date_str = args.from_date
    from_date = datetime.strptime(from_date_str, '%Y-%m-%d') if from_date_str else None
    commit_logs = gl.get_git_commit_logs(git_folder, from_date)

    if args.output_csv:
        output_csv = args.output_csv
        gl.save_to_csv(commit_logs, output_csv)
        print(f'Commit logs saved to {output_csv}')
        
        # Copy CSV content to clipboard
        with open(output_csv, 'r') as csv_file:
            csv_content = csv_file.read()
            gl.copy_to_clipboard(csv_content)
            print('CSV content copied to clipboard')

        # Open the specified URL with the CSV data
        web_url = f'https://mohan-chinnappan-n5.github.io/viz/datatable/dt.html?c=csv'
        webbrowser.open(web_url)

    else:
        print('Commit Logs:')
        for commit in commit_logs:
            print(commit)

main()
