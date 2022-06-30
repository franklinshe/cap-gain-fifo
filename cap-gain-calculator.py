#!/usr/bin/env python
# coding: utf-8

# # Capital Gain Calculator (FIFO Method)
# Adapted from VBA to Python
# 
# ### Structures
# 1. Transaction queue
#     - HashMap (key is Asset, dict is tuple of two deques)
# 2. Transactions
#     - 6 properties: Timestamp, Asset (eg. BTC), Type (buy or sell), Units, Total Amount ($), IRS ID (eg. Gemi 1)
# 
# ### Outline
# 1. Validate transaction log CSV
#     - Sort CSV by Timestamp property
#     - Ensure valid Type property and corresponding Units property sign
#     - Ensure other properties properties
# 2. Input data from transaction log CSV file into a buy transaction queue and sell transaction queue for each asset (eg. transaction log with BTC and ETH transactions -> BTC-Buy, BTC-Sell, ETH-Buy, Eth-Sell)
# 3. For each asset, run FIFO algorithm
#     - While there are still transactions in the sell transaction queue, match front of sell queue with front of buy queue transaction (verifying buy transaction Timestamp property is before sell transaction)
#     - When a match is found, add write buy-sell transaction to output CSV file
#     - Update remaining balance (Units and Total Amount property) on buy/sell transaction and/or remove empty transaction(s) from respective queue
#     - Run until the sell transaction queue is empty, the remaining buy transaction(s) are the carryover
# 4. Create summary report

# # 1. Read Input .xlsx File

# In[1]:


import sys
import ctypes
import pandas as pd
from collections import deque

print(pd.Timestamp.now(), "Reading input.xlsx")

# Read input xlsx sheet to transactions dataframe
transactions = pd.read_excel('input.xlsx')

# Sort transactions dataframe by Timestamp and then IRS ID
transactions = transactions.sort_values(by=['Timestamp', 'IRS ID'])


# # 2. Validate and Insert Data into Transaction Queues

# In[2]:
print(pd.Timestamp.now(), "Validating input.xlsx")

# Create Transaction Queues
asset_map = {}

# Define buy and sell constants
BUY, SELL = 0, 1

def validate_error(row_number, message):
    ctypes.windll.user32.MessageBoxW(None, "Row " + str(row_number) + ": " + message, 'Validation of input.xlsx Failed', 0)
    sys.exit("Row " + str(row_number) + ": " + message)

# Validate and insert into transaction queues
for index, row in transactions.iterrows():
    if pd.isnull(row['Asset']):
        validate_error(index + 2, "Asset is empty")
    if pd.isnull(row['Units']):
        validate_error(index + 2, "Units is empty")
    if pd.isnull(row['Total Amount']) or row['Total Amount'] <= 0:
        validate_error(index + 2, "Total Amount is empty or negative")
    if pd.isnull(row['Timestamp']) or row['Timestamp'] > pd.Timestamp.now():
        validate_error(index + 2, "Timestamp is empty or invalid")
    if pd.isnull(row['IRS ID']):
        validate_error(index + 2, "IRS ID is empty")

    # Create buy and sell deques for each asset
    asset = row['Asset']
    if asset not in asset_map:
        buy_deque = deque()
        sell_deque = deque()
        asset_map[asset] = (buy_deque, sell_deque)
    
    # Add transaction to respective deque
    if row['Type'] == "Buy":
        if row['Units'] < 0:
            validate_error(index + 2, "Units is negative on a Buy transaction")
        asset_map[asset][BUY].appendleft(row)
    elif row['Type'] == "Sell":
        if row['Units'] > 0:
            validate_error(index + 2, "Units is positive on a Sell transaction")
        asset_map[asset][SELL].appendleft(row)
    else:
        validate_error(index + 2, "Type is empty or neither Buy nor Sell")


# # 3. Run FIFO Transaction Matching

# In[3]:
print(pd.Timestamp.now(), "Running FIFO matching algorithm")

# Define fieldnames
fieldnames = ['Asset', 'Date Purchased', 'Date Sold', 'Units', 'Sale Price', 'Basis',
              'Gain / Loss', 'Remainder Units', 'Remainder Basis', 'IRS ID Buy', 'IRS ID Sell']

# Create dictionary with keys as asset and value as corresponding fifo dataframe
fifo = {}

# Create dataframe for possible margin transactions
margin = pd.DataFrame(columns = fieldnames)

# Create dictionary to store short and long term capital gain/loss statistics
year_summary = {}
year_summary['totals'] = {'Year': 'Totals', 'STCG': 0, 'STCL': 0, 'LTCG': 0, 'LTCL': 0, 'Net CG': 0}

# Loop through each asset
for asset in asset_map:
    
    # Create and set default match properties
    match = {}
    match['Asset'] = asset
    match['Remainder Units'] = '-'
    match['Remainder Basis'] = '-'
    
    # Create new fifo asset-dataframe pair for asset
    fifo[asset] = pd.DataFrame(columns = fieldnames)
    
    # Create dictionary to story volume statistics for asset
    volume_summary = {'Gain / Loss': 0, 'Sale Price': 0, 'Basis': 0, 'Remainder Units': 0, 'Remainder Basis': 0}
    
    # Loop until no more sell transactions for that asset
    while asset_map[asset][SELL]:
        
        # Pop earliest sell transaction
        sell_tx = asset_map[asset][SELL].pop()
        
        # Check if corresponding buy transaction has to be margin (exists or earliest is after sell)
        if not asset_map[asset][BUY] or \
               asset_map[asset][BUY][-1]['Timestamp'] > sell_tx['Timestamp'] + pd.Timedelta(15, "seconds"):
            # (Give 15 seconds leeway)
            
            # Create margin match and insert into margin dataframe
            match['IRS ID Buy'] = "MARGIN"
            match['IRS ID Sell'] = sell_tx['IRS ID']
            match['Date Purchased'] = sell_tx['Timestamp']
            match['Date Sold'] = sell_tx['Timestamp']
            match['Units'] = abs(sell_tx['Units'])
            match['Sale Price'] = sell_tx['Total Amount']
            match['Basis'] = 0
            match['Gain / Loss'] = match['Sale Price']
            
            match_df = pd.DataFrame([match])
            margin = pd.concat([margin, match_df], ignore_index=True)
        
        # Non-margin fifo match
        else:
            
            # Pop earliest buy transaction
            buy_tx = asset_map[asset][BUY].pop()

            # Populate match dictionary
            match['IRS ID Buy'] = buy_tx['IRS ID']
            match['IRS ID Sell'] = sell_tx['IRS ID']
            match['Date Purchased'] = buy_tx['Timestamp']
            match['Date Sold'] = sell_tx['Timestamp']

            # Sell transaction units are greater, so empty buy transaction
            if abs(sell_tx['Units']) > buy_tx['Units']:

                # Calculate pro rata sale price
                pro_rata_sale_price = buy_tx['Units'] / abs(sell_tx['Units']) * sell_tx['Total Amount']

                # Populate match dictionary
                match['Units'] = buy_tx['Units']
                match['Sale Price'] = pro_rata_sale_price
                match['Basis'] = buy_tx['Total Amount']

                # Update sell transaction information and put back into deque
                sell_tx['Units'] = sell_tx['Units'] + buy_tx['Units']
                sell_tx['Total Amount'] = sell_tx['Total Amount'] - pro_rata_sale_price
                asset_map[asset][SELL].append(sell_tx)

            # Buy transaction units are greater, so empty sell transaction
            elif abs(sell_tx['Units']) < buy_tx['Units']:

                # Calculate pro rata basis
                pro_rata_basis = abs(sell_tx['Units']) / buy_tx['Units'] * buy_tx['Total Amount']

                # Populate match dictionary
                match['Units'] = abs(sell_tx['Units'])
                match['Sale Price'] = sell_tx['Total Amount']
                match['Basis'] = pro_rata_basis

                # Update buy transaction information and put back into deque
                buy_tx['Units'] = buy_tx['Units'] + sell_tx['Units']
                buy_tx['Total Amount'] = buy_tx['Total Amount'] - pro_rata_basis
                asset_map[asset][BUY].append(buy_tx)

            # Transaction units are the same
            else:
                # Populate match dictionary
                match['Units'] = buy_tx['Units']
                match['Sale Price'] = sell_tx['Total Amount']
                match['Basis'] = buy_tx['Total Amount']
        
        # Calculate match gain or loss
        match['Gain / Loss'] = match['Sale Price'] - match['Basis']
        
        # Add match to fifo dataframe if match units are not dust
        if abs(match['Units']) > .00000001:
            match_df = pd.DataFrame([match])
            fifo[asset] = pd.concat([fifo[asset], match_df], ignore_index=True)
        
        # Update year by year summary statistics
        year = str(sell_tx['Timestamp'].year)
        
        if year not in year_summary:
            year_summary[year] = {'Year': year, 'STCG': 0, 'STCL': 0, 'LTCG': 0, 'LTCL': 0, 'Net CG': 0}
        
        if sell_tx['Timestamp'] - buy_tx['Timestamp'] < pd.Timedelta(365, "d"):
            if match['Gain / Loss'] > 0:
                year_summary[year]['STCG'] += match['Gain / Loss']
                year_summary['totals']['STCG'] += match['Gain / Loss']
            else:
                year_summary[year]['STCL'] += match['Gain / Loss']
                year_summary['totals']['STCL'] += match['Gain / Loss']
        else:
            if match['Gain / Loss'] > 0:
                year_summary[year]['LTCG'] += match['Gain / Loss']
                year_summary['totals']['LTCG'] += match['Gain / Loss']
            else:
                year_summary[year]['LTCL'] += match['Gain / Loss']
                year_summary['totals']['LTCL'] += match['Gain / Loss']
                
        year_summary[year]['Net CG'] += match['Gain / Loss']
        year_summary['totals']['Net CG'] += match['Gain / Loss']
        
        # Update volume and total capital gain statistics
        volume_summary['Gain / Loss'] += match['Gain / Loss']
        volume_summary['Sale Price'] += match['Sale Price']
        volume_summary['Basis'] += match['Basis']
    
    
    # The asset carryover are the remaining transactions in the buy deque
    while asset_map[asset][BUY]:
        buy_tx = asset_map[asset][BUY].pop()
              
        # Populate match as a carryover
        match['IRS ID Buy'] = buy_tx['IRS ID']
        match['IRS ID Sell'] = '-'
        match['Date Purchased'] = buy_tx['Timestamp']
        match['Date Sold'] = '-'
        match['Units'] = '-'
        match['Remainder Units'] = buy_tx['Units']
        match['Sale Price'] = '-'
        match['Basis'] = '-'
        match['Remainder Basis'] = buy_tx['Total Amount']
        match['Gain / Loss'] = '-'

        volume_summary['Remainder Units'] += buy_tx['Units']
        volume_summary['Remainder Basis'] += buy_tx['Total Amount']
        
        # Add carryover to fifo dataframe if carryover units are not dust
        if buy_tx['Units'] > .00000001:
            match_df = pd.DataFrame([match])
            fifo[asset] = pd.concat([fifo[asset], match_df], ignore_index=True)

    # Add volume summary to fifo dataframe
    volume_df = pd.DataFrame([volume_summary])
    fifo[asset] = pd.concat([fifo[asset], volume_df], ignore_index=True)

# Create summary dataframe from dictionary
summary = pd.DataFrame.from_dict(year_summary, orient='index')
summary = summary.sort_values(by='Year')


# # 4. Write and format dataframes into .xlsx file

# In[4]:
print(pd.Timestamp.now(), "Creating output.xlsx")

with pd.ExcelWriter('output.xlsx', engine='xlsxwriter') as writer:
    transactions.to_excel(writer, sheet_name='Input', index = False)
    for asset in fifo:
        fifo[asset].to_excel(writer, sheet_name=asset + ' FIFO', index = False)
    summary.to_excel(writer, sheet_name='Summary', index = False)
    margin.to_excel(writer, sheet_name='Margin', index = False)
       
    workbook  = writer.book
    
    currency_format = workbook.add_format({'num_format': '#,##0.00_);[Red](#,##0.00)'})
    unit_format = workbook.add_format({'num_format': '#,##0.00000000_);[Red](#,##0.00000000)'})
    
    worksheet = writer.sheets['Input']
    worksheet.freeze_panes(1, 0)
    worksheet.set_column(0, 0, 20)
    worksheet.set_column(1, 2, 5)
    worksheet.set_column('D:D', 16, unit_format)
    worksheet.set_column('E:E', 16, currency_format)
    worksheet.set_column(5, 5, 12)

    for asset in fifo:
        worksheet = writer.sheets[asset + ' FIFO']
        worksheet.freeze_panes(1, 0)
        worksheet.set_column(0, 0, 5)
        worksheet.set_column(1, 2, 20)
        worksheet.set_column('D:D', 16, unit_format)
        worksheet.set_column('E:G', 16, currency_format)
        worksheet.set_column('H:I', 16, unit_format)
        worksheet.set_column(9, 10, 12)

    worksheet = writer.sheets['Summary']
    worksheet.freeze_panes(1, 0)
    worksheet.set_column(0, 0, 5)
    worksheet.set_column(1, 5, 12, currency_format)
    
    worksheet = writer.sheets['Margin']
    worksheet.freeze_panes(1, 0)
    worksheet.set_column(0, 0, 5)
    worksheet.set_column(1, 2, 20)
    worksheet.set_column('D:D', 16, unit_format)
    worksheet.set_column('E:G', 16, currency_format)
    worksheet.set_column('H:I', 16, unit_format)
    worksheet.set_column(9, 10, 12)

