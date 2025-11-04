# Transaction Log Feature

## Overview
A new `/transactions` endpoint has been added to the Flask application that displays a comprehensive log of all transactions pulled from Akahu.

## Features
- **Transaction Display**: Shows Date, Payee, Memo, and Amount for all transactions
- **Multi-Account Support**: Aggregates transactions from all mapped Akahu accounts
- **Formatted Display**: Clean, sortable HTML table with styling
- **Navigation**: Easy navigation links to other application endpoints

## Usage

### Starting the Flask App
```bash
python flask_app.py
```

### Accessing the Transaction Log
Once the Flask app is running, navigate to:
```
http://localhost:5000/transactions
```

Or use the navigation from the home page at `http://localhost:5000/`

## What the Transaction Log Shows

The transaction log displays:
1. **Date**: Transaction date and time (formatted as YYYY-MM-DD HH:MM:SS)
2. **Account**: The Akahu account name the transaction belongs to
3. **Payee**: The merchant name or description
4. **Memo**: Additional transaction description/notes
5. **Amount**: Transaction amount in dollars (formatted as $X.XX)

## Transaction Data Source

The transactions are fetched from:
- All mapped Akahu accounts that aren't configured to be skipped
- Transactions since the last sync timestamp for each account
- Uses the same `get_all_akahu()` function that powers the sync operations

## Technical Details

### Implementation Location
- Main route added in: `modules/webhook_handler.py`
- Uses: `get_all_akahu()` from `modules/transaction_handler.py`

### Key Functions
- `get_all_akahu()`: Fetches transactions from Akahu API with pagination
- Processes and formats transaction data from pandas DataFrames
- Renders HTML using Flask's `render_template_string()`

## UI Features
- **Responsive Table**: Full-width table with hover effects
- **Sticky Headers**: Column headers stay visible when scrolling
- **Summary Section**: Shows total transaction count
- **Navigation Links**: Quick access to Status, Sync, and other endpoints
- **Clean Styling**: Professional appearance with green accents

## Error Handling
- Gracefully handles missing transaction data
- Logs errors for individual account fetch failures
- Continues processing other accounts if one fails
- Returns appropriate error responses with HTTP status codes
