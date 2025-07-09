import pandas as pd
import gzip
import argparse
from datetime import datetime
import os
import glob
from pytz import UTC
import multiprocessing as mp
from functools import partial
import time

def read_savings_plan_ids(file_path='sp-id'):
    """Read Savings Plan IDs from file."""
    try:
        with open(file_path, 'r') as f:
            # Read lines and strip whitespace, filter out empty lines
            ids = [line.strip() for line in f if line.strip()]
        if not ids:
            raise ValueError("No Savings Plan IDs found in the file")
        print(f"Found {len(ids)} Savings Plan IDs:")
        for sp_id in ids:
            print(f"- {sp_id}")
        return ids
    except FileNotFoundError:
        raise FileNotFoundError(f"Savings Plan IDs file '{file_path}' not found")

def read_cur_file(file_path):
    """Read and parse the CUR gzip file."""
    print(f"Reading CUR file: {file_path}")
    with gzip.open(file_path, 'rt') as f:
        # Read with low_memory=False to avoid dtype warnings
        df = pd.read_csv(f, low_memory=False)

    # Print column names for debugging
    print("\nAvailable columns in the CUR file:")
    for col in df.columns:
        print(f"- {col}")

    return df

def process_single_file(file_path, savings_plan_ids):
    """Process a single CUR file and return the results."""
    try:
        df = read_cur_file(file_path)
        return analyze_savings_plans(df, savings_plan_ids)
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None

def analyze_savings_plans(df, savings_plan_ids):
    """Analyze Savings Plans usage and savings."""
    # Define possible column names for different CUR formats
    line_item_type_cols = [
        'line_item_line_item_type',
        'lineItem/LineItemType',
        'LineItemType',
        'lineItemType'
    ]

    usage_account_cols = [
        'line_item_usage_account_id',
        'lineItem/UsageAccountId',
        'UsageAccountId',
        'usageAccountId'
    ]

    usage_amount_cols = [
        'line_item_usage_amount',
        'lineItem/UsageAmount',
        'UsageAmount',
        'usageAmount'
    ]

    unblended_cost_cols = [
        'line_item_unblended_cost',
        'lineItem/UnblendedCost',
        'UnblendedCost',
        'unblendedCost'
    ]

    savings_plan_cost_cols = [
        'savings_plan_savings_plan_effective_cost',
        'savingsPlan/SavingsPlanEffectiveCost',
        'SavingsPlanEffectiveCost',
        'savingsPlanEffectiveCost'
    ]

    savings_plan_id_cols = [
        'savings_plan_savings_plan_a_r_n',
        'lineItem/SavingsPlanId',
        'SavingsPlanId',
        'savingsPlanId'
    ]

    usage_start_date_cols = [
        'line_item_usage_start_date',
        'lineItem/UsageStartDate',
        'UsageStartDate',
        'usageStartDate'
    ]

    bill_payer_cols = [
        'bill_payer_account_id',
        'bill/PayerAccountId',
        'PayerAccountId',
        'payerAccountId'
    ]

    usage_type_cols = [
        'line_item_usage_type',
        'lineItem/UsageType',
        'UsageType',
        'usageType'
    ]

    # Find the correct column names
    line_item_type_col = next((col for col in line_item_type_cols if col in df.columns), None)
    usage_account_col = next((col for col in usage_account_cols if col in df.columns), None)
    usage_amount_col = next((col for col in usage_amount_cols if col in df.columns), None)
    unblended_cost_col = next((col for col in unblended_cost_cols if col in df.columns), None)
    savings_plan_cost_col = next((col for col in savings_plan_cost_cols if col in df.columns), None)
    savings_plan_id_col = next((col for col in savings_plan_id_cols if col in df.columns), None)
    usage_start_date_col = next((col for col in usage_start_date_cols if col in df.columns), None)
    bill_payer_col = next((col for col in bill_payer_cols if col in df.columns), None)
    usage_type_col = next((col for col in usage_type_cols if col in df.columns), None)

    # Validate required columns
    missing_cols = []
    for col_name, col in [
        ('LineItemType', line_item_type_col),
        ('UsageAccountId', usage_account_col),
        ('UsageAmount', usage_amount_col),
        ('UnblendedCost', unblended_cost_col),
        ('SavingsPlanEffectiveCost', savings_plan_cost_col),
        ('SavingsPlanId', savings_plan_id_col),
        ('UsageStartDate', usage_start_date_col),
        ('PayerAccountId', bill_payer_col),
        ('UsageType', usage_type_col)
    ]:
        if col is None:
            missing_cols.append(col_name)

    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    # Convert usage start date to datetime with UTC timezone
    df[usage_start_date_col] = pd.to_datetime(df[usage_start_date_col])

    # Filter for April 2025 (using UTC timezone)
    april_start = pd.Timestamp('2025-06-01', tz=UTC)
    april_end = pd.Timestamp('2025-06-30 23:59:59.999999', tz=UTC)
    df = df[(df[usage_start_date_col] >= april_start) & (df[usage_start_date_col] <= april_end)]

    # Filter for Savings Plans usage
    sp_df = df[df[line_item_type_col] == 'SavingsPlanCoveredUsage']

    # Filter for specified Savings Plans
    sp_df = sp_df[sp_df[savings_plan_id_col].apply(lambda x: any(sp_id in str(x) for sp_id in savings_plan_ids))]

    # Group by account, savings plan ID, payer account, and usage type
    results = sp_df.groupby([usage_account_col, savings_plan_id_col, bill_payer_col, usage_type_col]).agg({
        usage_amount_col: 'sum',
        unblended_cost_col: 'sum',
        savings_plan_cost_col: 'sum'
    }).reset_index()

    # Calculate savings
    results['Savings'] = results[unblended_cost_col] - results[savings_plan_cost_col]

    # Rename columns for clarity
    results.columns = [
        'Account ID',
        'Savings Plan ID',
        'Purchaser Account ID',
        'Usage Type',
        'Usage Amount',
        'On-Demand Cost',
        'Effective Cost',
        'Savings'
    ]

    return results

def generate_detailed_csv(results, output_file):
    """Generate detailed CSV report with account summaries and details."""
    # Create account summary
    account_summary = results.groupby(['Account ID', 'Purchaser Account ID']).agg({
        'Usage Amount': 'sum',
        'On-Demand Cost': 'sum',
        'Effective Cost': 'sum',
        'Savings': 'sum'
    }).reset_index()

    # Create the final dataframe with detailed records
    final_df = results.copy()

    # Add grand total row
    grand_total = {
        'Account ID': 'GRAND TOTAL',
        'Purchaser Account ID': '',
        'Savings Plan ID': '',
        'Usage Type': '',
        'Usage Amount': account_summary['Usage Amount'].sum(),
        'On-Demand Cost': account_summary['On-Demand Cost'].sum(),
        'Effective Cost': account_summary['Effective Cost'].sum(),
        'Savings': account_summary['Savings'].sum()
    }

    # Add grand total to the final dataframe
    final_df = pd.concat([
        final_df,
        pd.DataFrame([grand_total])
    ], ignore_index=True)

    # Format numbers
    final_df['Usage Amount'] = final_df['Usage Amount'].map('{:.2f}'.format)
    final_df['On-Demand Cost'] = final_df['On-Demand Cost'].map('${:.2f}'.format)
    final_df['Effective Cost'] = final_df['Effective Cost'].map('${:.2f}'.format)
    final_df['Savings'] = final_df['Savings'].map('${:.2f}'.format)

    # Save to CSV
    final_df.to_csv(output_file, index=False)
    print(f"\nDetailed CSV report saved to: {output_file}")

def find_cur_files():
    """Find all CUR files in directories starting with '2025'."""
    cur_files = []
    # Find all directories starting with '2025'
    for dir_path in glob.glob('2025*'):
        if os.path.isdir(dir_path):
            # Find all .csv.gz files in the directory
            files = glob.glob(os.path.join(dir_path, '*.csv.gz'))
            cur_files.extend(files)

    if not cur_files:
        raise FileNotFoundError("No CUR files found in directories starting with '2025'")

    print(f"Found {len(cur_files)} CUR files:")
    for file in cur_files:
        print(f"- {file}")

    return cur_files

def main():
    parser = argparse.ArgumentParser(description='Analyze AWS CUR data for Savings Plans usage')
    parser.add_argument('--sp-id-file', default='sp-id',
                      help='Path to the file containing Savings Plan IDs (default: sp-id)')
    parser.add_argument('--output', help='Output file path (default: savings_plans_analysis.csv)')
    parser.add_argument('--processes', type=int, default=mp.cpu_count(),
                      help='Number of processes to use (default: number of CPU cores)')

    args = parser.parse_args()

    # Read Savings Plan IDs from file
    savings_plan_ids = read_savings_plan_ids(args.sp_id_file)

    # Set default output file if not specified
    if not args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = f'savings_plans_analysis_april_2025_{timestamp}.csv'

    # Find all CUR files
    cur_files = find_cur_files()

    # Start timing
    start_time = time.time()

    # Process files in parallel
    print(f"\nProcessing {len(cur_files)} files using {args.processes} processes...")
    with mp.Pool(processes=args.processes) as pool:
        # Create a partial function with the savings_plan_ids argument
        process_func = partial(process_single_file, savings_plan_ids=savings_plan_ids)
        # Process files in parallel
        results = pool.map(process_func, cur_files)

    # Filter out None results (from failed processing)
    results = [r for r in results if r is not None]

    if not results:
        raise ValueError("No valid results found after processing files")

    # Combine results from all files
    combined_results = pd.concat(results, ignore_index=True)

    # Aggregate results by Account ID, Savings Plan ID, Purchaser Account ID, and Usage Type
    print("\nAggregating results by Account ID, Savings Plan ID, Purchaser Account ID, and Usage Type...")
    combined_results = combined_results.groupby(['Account ID', 'Savings Plan ID', 'Purchaser Account ID', 'Usage Type']).agg({
        'Usage Amount': 'sum',
        'On-Demand Cost': 'sum',
        'Effective Cost': 'sum',
        'Savings': 'sum'
    }).reset_index()

    # Sort results by Account ID, Purchaser Account ID, Savings Plan ID, and Usage Type
    combined_results = combined_results.sort_values(['Account ID', 'Purchaser Account ID', 'Savings Plan ID', 'Usage Type'])

    # Generate detailed CSV report
    generate_detailed_csv(combined_results, args.output)

    # Print summary to console
    account_summary = combined_results.groupby(['Account ID', 'Purchaser Account ID']).agg({
        'Usage Amount': 'sum',
        'On-Demand Cost': 'sum',
        'Effective Cost': 'sum',
        'Savings': 'sum'
    }).reset_index()

    # Calculate and print processing time
    processing_time = time.time() - start_time
    print(f"\nProcessing completed in {processing_time:.2f} seconds")

    print("\nOverall Summary (May 2025):")
    print("=" * 80)
    print(f"Total accounts analyzed: {len(account_summary)}")
    print(f"Total usage amount: {account_summary['Usage Amount'].sum():.2f}")
    print(f"Total on-demand cost: ${account_summary['On-Demand Cost'].sum():.2f}")
    print(f"Total effective cost: ${account_summary['Effective Cost'].sum():.2f}")
    print(f"Total savings: ${account_summary['Savings'].sum():.2f}")

    # Print per-account summary
    print("\nPer-Account Summary:")
    print("=" * 80)
    for _, account in account_summary.iterrows():
        print(f"\nAccount ID: {account['Account ID']}")
        print(f"Purchaser Account ID: {account['Purchaser Account ID']}")
        print(f"Total Usage Amount: {account['Usage Amount']:.2f}")
        print(f"Total On-Demand Cost: ${account['On-Demand Cost']:.2f}")
        print(f"Total Effective Cost: ${account['Effective Cost']:.2f}")
        print(f"Total Savings: ${account['Savings']:.2f}")

        # Print Savings Plan details for this account
        account_details = combined_results[
            (combined_results['Account ID'] == account['Account ID']) &
            (combined_results['Purchaser Account ID'] == account['Purchaser Account ID'])
        ]
        print("\nSavings Plan Details:")
        for _, detail in account_details.iterrows():
            print(f"  Savings Plan ID: {detail['Savings Plan ID']}")
            print(f"  Usage Type: {detail['Usage Type']}")
            print(f"  Usage Amount: {detail['Usage Amount']:.2f}")
            print(f"  On-Demand Cost: ${detail['On-Demand Cost']:.2f}")
            print(f"  Effective Cost: ${detail['Effective Cost']:.2f}")
            print(f"  Savings: ${detail['Savings']:.2f}")

if __name__ == '__main__':
    main()