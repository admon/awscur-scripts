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

def read_cur_file(file_path):
    print(f"Reading CUR file: {file_path}")
    with gzip.open(file_path, 'rt') as f:
        df = pd.read_csv(f, low_memory=False)
    print("Columns in file:", df.columns.tolist())  # <-- Debug print
    return df

def process_single_file(file_path):
    try:
        df = read_cur_file(file_path)
        return analyze_ri_savings(df)
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None

def process_single_file_with_ri(file_path, ri_id_set):
    try:
        df = read_cur_file(file_path)
        return analyze_ri_savings(df, ri_id_set=ri_id_set)
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None

def analyze_ri_savings(df, ri_id_set=None):
    # Column name candidates
    line_item_type_cols = [
        'line_item_line_item_type', 'lineItem/LineItemType', 'LineItemType', 'lineItemType'
    ]
    ri_subscription_id_cols = [
        'reservation_subscription_id', 'reservation/SubscriptionId', 'SubscriptionId', 'subscriptionId'
    ]
    ri_arn_cols = [
        'reservation_reservation_a_r_n', 'reservation/ReservationARN', 'ReservationARN', 'reservationArn'
    ]
    bill_payer_cols = [
        'bill_payer_account_id', 'bill/PayerAccountId', 'PayerAccountId', 'payerAccountId'
    ]
    usage_account_cols = [
        'line_item_usage_account_id', 'lineItem/UsageAccountId', 'UsageAccountId', 'usageAccountId'
    ]
    usage_amount_cols = [
        'line_item_usage_amount', 'lineItem/UsageAmount', 'UsageAmount', 'usageAmount'
    ]
    public_ondemand_cost_cols = [
        'pricing_public_on_demand_cost', 'pricing/PublicOnDemandCost', 'PublicOnDemandCost', 'publicOnDemandCost',
        # Add your actual column name here after checking the debug output
        # e.g. 'lineItem/PublicOnDemandCost'
    ]
    ri_effective_cost_cols = [
        'reservation_effective_cost', 'reservation/EffectiveCost', 'EffectiveCost', 'effectiveCost'
    ]
    rifee_cost_cols = [
        'reservation_recurring_fee_for_usage', # <-- Added based on your columns
        'line_item_unblended_cost', 'lineItem/UnblendedCost', 'UnblendedCost', 'unblendedCost'
    ]
    usage_start_date_cols = [
        'line_item_usage_start_date', 'lineItem/UsageStartDate', 'UsageStartDate', 'usageStartDate'
    ]

    # Find actual column names
    line_item_type_col = next((col for col in line_item_type_cols if col in df.columns), None)
    ri_subscription_id_col = next((col for col in ri_subscription_id_cols if col in df.columns), None)
    ri_arn_col = next((col for col in ri_arn_cols if col in df.columns), None)
    bill_payer_col = next((col for col in bill_payer_cols if col in df.columns), None)
    usage_account_col = next((col for col in usage_account_cols if col in df.columns), None)
    usage_amount_col = next((col for col in usage_amount_cols if col in df.columns), None)
    public_ondemand_cost_col = next((col for col in public_ondemand_cost_cols if col in df.columns), None)
    ri_effective_cost_col = next((col for col in ri_effective_cost_cols if col in df.columns), None)
    rifee_cost_col = next((col for col in rifee_cost_cols if col in df.columns), None)
    usage_start_date_col = next((col for col in usage_start_date_cols if col in df.columns), None)

    missing_cols = []
    for col_name, col in [
        ('LineItemType', line_item_type_col),
        ('SubscriptionId', ri_subscription_id_col),
        ('ReservationARN', ri_arn_col),
        ('PayerAccountId', bill_payer_col),
        ('UsageAccountId', usage_account_col),
        ('UsageAmount', usage_amount_col),
        ('PublicOnDemandCost', public_ondemand_cost_col),
        ('EffectiveCost', ri_effective_cost_col),
        ('RIFeeCost', rifee_cost_col),
        ('UsageStartDate', usage_start_date_col)
    ]:
        if col is None:
            missing_cols.append(col_name)
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    # Warn if public on-demand cost column is all zeros
    if public_ondemand_cost_col and df[public_ondemand_cost_col].sum() == 0:
        print(f"Warning: The public on-demand cost column '{public_ondemand_cost_col}' is all zeros. Please check if this is correct.")

    # Convert usage start date to datetime with UTC timezone
    df[usage_start_date_col] = pd.to_datetime(df[usage_start_date_col])

    # Filter for May 2025 (using UTC timezone)
    may_start = pd.Timestamp('2025-06-01', tz=UTC)
    may_end = pd.Timestamp('2025-06-30 23:59:59.999999', tz=UTC)
    df = df[(df[usage_start_date_col] >= may_start) & (df[usage_start_date_col] <= may_end)]

    # Filter by RI IDs if provided
    if ri_id_set is not None:
        # Only keep rows where the RI ID (e.g. ri-2025-05-23-11-40-46-977) is in reservation_reservation_a_r_n
        df = df[df[ri_arn_col].apply(lambda x: any(ri_id in str(x) for ri_id in ri_id_set))]

    # DiscountedUsage: RI分摊用量
    du_df = df[df[line_item_type_col] == 'DiscountedUsage']
    du_df = du_df[du_df[ri_subscription_id_col].notnull()]
    du_grouped = du_df.groupby([ri_subscription_id_col, ri_arn_col, usage_account_col]).agg({
        public_ondemand_cost_col: 'sum',
        ri_effective_cost_col: 'sum'
    }).reset_index()
    du_grouped.columns = [
        'reservation_subscription_id',
        'reservation_reservation_a_r_n',
        'Usage Account ID',
        'On-Demand Cost',
        'RI Effective Cost'
    ]

    # 计算节省
    du_grouped['Savings'] = du_grouped['On-Demand Cost'] - du_grouped['RI Effective Cost']

    return du_grouped

def generate_detailed_csv(results, output_file):
    final_df = results.copy()
    grand_total = {
        'reservation_subscription_id': 'GRAND TOTAL',
        'reservation_reservation_a_r_n': '',
        'Usage Account ID': '',
        'On-Demand Cost': final_df['On-Demand Cost'].sum(),
        'RI Effective Cost': final_df['RI Effective Cost'].sum(),
        'Savings': final_df['Savings'].sum()
    }
    final_df = pd.concat([
        final_df,
        pd.DataFrame([grand_total])
    ], ignore_index=True)
    final_df['On-Demand Cost'] = final_df['On-Demand Cost'].map('${:.2f}'.format)
    final_df['RI Effective Cost'] = final_df['RI Effective Cost'].map('${:.2f}'.format)
    final_df['Savings'] = final_df['Savings'].map('${:.2f}'.format)
    # Remove 'reservation_subscription_id' from output
    final_df = final_df.drop(columns=['reservation_subscription_id'])
    final_df.to_csv(output_file, index=False)
    print(f"\nDetailed CSV report saved to: {output_file}")

def find_cur_files():
    cur_files = []
    for dir_path in glob.glob('2025*'):
        if os.path.isdir(dir_path):
            files = glob.glob(os.path.join(dir_path, '*.csv.gz'))
            cur_files.extend(files)
    if not cur_files:
        raise FileNotFoundError("No CUR files found in directories starting with '2025'")
    print(f"Found {len(cur_files)} CUR files:")
    for file in cur_files:
        print(f"- {file}")
    return cur_files

def main():
    parser = argparse.ArgumentParser(description='Analyze AWS CUR data for RI savings (precise)')
    parser.add_argument('--output', help='Output file path (default: ri_savings_analysis_may_2025.csv)')
    parser.add_argument('--processes', type=int, default=mp.cpu_count(), help='Number of processes to use (default: number of CPU cores)')
    args = parser.parse_args()
    if not args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = f'ri_savings_analysis_may_2025_{timestamp}.csv'
    cur_files = find_cur_files()

    # Read RI IDs from file 'ri-id'
    if os.path.exists('ri-id'):
        with open('ri-id', 'r') as f:
            ri_id_set = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(ri_id_set)} RI IDs from ri-id file.")
    else:
        print("ri-id file not found. Will not filter by RI IDs.")
        ri_id_set = None

    start_time = time.time()
    print(f"\nProcessing {len(cur_files)} files using {args.processes} processes...")
    with mp.Pool(processes=args.processes) as pool:
        results = pool.map(partial(process_single_file_with_ri, ri_id_set=ri_id_set), cur_files)
    results = [r for r in results if r is not None]
    if not results:
        raise ValueError("No valid results found after processing files")
    combined_results = pd.concat(results, ignore_index=True)
    # Aggregate by reservation_subscription_id, ARN, Usage Account ID
    print("\nAggregating results by reservation_subscription_id, reservation_reservation_a_r_n, Usage Account ID...")
    combined_results = combined_results.groupby(['reservation_subscription_id', 'reservation_reservation_a_r_n', 'Usage Account ID']).agg({
        'On-Demand Cost': 'sum',
        'RI Effective Cost': 'sum',
        'Savings': 'sum'
    }).reset_index()
    combined_results = combined_results.sort_values(['reservation_subscription_id', 'reservation_reservation_a_r_n', 'Usage Account ID'])
    generate_detailed_csv(combined_results, args.output)
    processing_time = time.time() - start_time
    print(f"\nProcessing completed in {processing_time:.2f} seconds")
    print("\nOverall Summary (May 2025):")
    print("=" * 80)
    n_ri = len(combined_results) - 1 if len(combined_results) > 0 else 0
    print(f"Total RI subscriptions analyzed: {n_ri}")
    print(f"Total on-demand cost: ${combined_results['On-Demand Cost'][:-1].sum():.2f}")
    print(f"Total RI effective cost: ${combined_results['RI Effective Cost'][:-1].sum():.2f}")
    print(f"Total savings: ${combined_results['Savings'][:-1].sum():.2f}")

if __name__ == '__main__':
    main()
