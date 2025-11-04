"""Module for handling webhook operations and Flask app creation."""

import base64
import logging
import sys
from flask import Flask, request, jsonify, redirect, url_for, render_template_string
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import pandas as pd
from datetime import datetime

from modules.account_mapper import load_existing_mapping
from modules.config import RUN_SYNC_TO_AB, RUN_SYNC_TO_YNAB, YNAB_ENDPOINT, YNAB_HEADERS, AKAHU_ENDPOINT, AKAHU_HEADERS
from modules.sync_handler import sync_to_ab, sync_to_ynab
from modules.sync_status import generate_sync_report
from modules.transaction_handler import (
    load_transactions_into_actual,
    clean_txn_for_ynab,
    load_transactions_into_ynab,
    create_adjustment_txn_ynab,
    get_all_akahu,
)
from modules.account_fetcher import get_akahu_balance, get_ynab_balance
from modules.transaction_tester import run_transaction_tests


def verify_signature(public_key: str, signature: str, request_body: bytes) -> None:
    """Verify that the request body has been signed by Akahu."""
    public_key = serialization.load_pem_public_key(public_key.encode("utf-8"))
    public_key.verify(
        base64.b64decode(signature), request_body, padding.PKCS1v15(), hashes.SHA256()
    )


def create_flask_app(actual_client, mapping_list, env_vars):
    """Create and configure Flask application for webhook handling."""
    app = Flask(__name__)

    @app.route("/test", methods=["GET"])
    def test_transactions():
        """Test endpoint to validate transaction handling."""
        try:
            result = run_transaction_tests(actual_client, mapping_list, env_vars)
            return jsonify(result), 200
        except Exception as e:
            logging.error(f"\n=== Test Failed ===\nError in test endpoint: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/")
    def root():
        """Root endpoint shows deprecation notice and status."""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Akahu to Budget Sync</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }
                .container {
                    background-color: #fff;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    max-width: 800px;
                }
                h1 {
                    color: #333;
                }
                .links {
                    margin-top: 20px;
                }
                .links a {
                    display: block;
                    margin: 10px 0;
                    padding: 10px;
                    background-color: #4CAF50;
                    color: white;
                    text-decoration: none;
                    border-radius: 3px;
                    text-align: center;
                }
                .links a:hover {
                    background-color: #45a049;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Akahu to Budget Sync</h1>
                <p>Welcome to the Akahu to Budget sync service.</p>
                
                <div class="links">
                    <a href="/status">Check Status</a>
                    <a href="/sync">Run Full Sync</a>
                    <a href="/transactions">View Transaction Log</a>
                    <a href="/test">Run Tests</a>
                </div>
            </div>
        </body>
        </html>
        """
        return html

    @app.route("/sync", methods=["GET"])
    def run_full_sync():
        """Run a full sync of all accounts."""
        errors = []
        actual_count = 0
        ynab_count = 0

        try:
            _, _, _, mapping_list = load_existing_mapping()

            if RUN_SYNC_TO_AB:
                actual_client.download_budget()
                actual_count = sync_to_ab(actual_client, mapping_list)

            if RUN_SYNC_TO_YNAB:
                ynab_count = sync_to_ynab(mapping_list)

            return generate_sync_report(mapping_list, actual_count, ynab_count)

        except Exception as e:
            logging.error(f"Sync failed: {str(e)}")
            return (
                jsonify(
                    {
                        "status": "error",
                        "error": str(e),
                    }
                ),
                500,
            )

    @app.route("/status", methods=["GET"])
    def status():
        """Endpoint to check if the webhook server is running."""
        return jsonify({"status": "Webhook server is running"}), 200

    @app.route("/transactions", methods=["GET"])
    def view_transactions():
        """Display a log of all transactions pulled from Akahu."""
        try:
            _, _, _, mapping_list = load_existing_mapping()
            
            all_transactions = []
            
            # Iterate through all mapped accounts
            for akahu_account_id, mapping_entry in mapping_list.items():
                akahu_account_name = mapping_entry.get("akahu_name", "Unknown Account")
                last_synced = mapping_entry.get(
                    "actual_synced_datetime",
                    mapping_entry.get("ynab_synced_datetime", "2025-05-05T00:00:00Z")
                )
                
                # Skip if account is configured to not be mapped
                if mapping_entry.get("actual_do_not_map") and mapping_entry.get("ynab_do_not_map"):
                    continue
                
                # Fetch transactions from Akahu
                try:
                    transactions_df = get_all_akahu(
                        akahu_account_id,
                        AKAHU_ENDPOINT,
                        AKAHU_HEADERS,
                        last_synced
                    )
                    
                    if transactions_df is not None and not transactions_df.empty:
                        # Add account name to each transaction
                        transactions_df['account_name'] = akahu_account_name
                        all_transactions.append(transactions_df)
                        
                except Exception as e:
                    logging.error(f"Error fetching transactions for {akahu_account_name}: {str(e)}")
                    continue
            
            # Combine all transactions
            if all_transactions:
                combined_df = pd.concat(all_transactions, ignore_index=True)
                
                # Extract relevant fields and format
                transaction_list = []
                for _, row in combined_df.iterrows():
                    # Parse date
                    date_str = row.get('date', row.get('created_at', ''))
                    if date_str:
                        try:
                            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            formatted_date = date_str
                    else:
                        formatted_date = 'N/A'
                    
                    # Extract payee - handle merchant being dict, None, or other type
                    merchant = row.get('merchant')
                    if isinstance(merchant, dict):
                        payee = merchant.get('name', row.get('description', 'N/A'))
                    else:
                        payee = row.get('description', 'N/A')
                    
                    # Extract transaction details
                    transaction_list.append({
                        'date': formatted_date,
                        'account': row.get('account_name', 'Unknown'),
                        'payee': payee,
                        'memo': row.get('description', ''),
                        'amount': f"${row.get('amount', 0):.2f}"
                    })
                
                # Sort by date (newest first)
                transaction_list.sort(key=lambda x: x['date'], reverse=True)
                
                # Extract unique account names for dropdown
                unique_accounts = sorted(list(set(txn['account'] for txn in transaction_list)))
            else:
                transaction_list = []
                unique_accounts = []
            
            # HTML template for displaying transactions
            html_template = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Akahu Transaction Log</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        background-color: #f5f5f5;
                    }
                    h1 {
                        color: #333;
                    }
                    .summary {
                        background-color: #fff;
                        padding: 15px;
                        margin-bottom: 20px;
                        border-radius: 5px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    .filter-box {
                        background-color: #fff;
                        padding: 15px;
                        margin-bottom: 20px;
                        border-radius: 5px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    .filter-row {
                        display: flex;
                        gap: 15px;
                        margin-bottom: 10px;
                    }
                    .filter-row > div {
                        flex: 1;
                    }
                    .filter-box input, .filter-box select {
                        width: 100%;
                        padding: 10px;
                        font-size: 16px;
                        border: 1px solid #ddd;
                        border-radius: 3px;
                        box-sizing: border-box;
                    }
                    .filter-box label {
                        display: block;
                        margin-bottom: 5px;
                        font-weight: bold;
                    }
                    table {
                        width: 100%;
                        border-collapse: collapse;
                        background-color: #fff;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    th {
                        background-color: #4CAF50;
                        color: white;
                        padding: 12px;
                        text-align: left;
                        position: sticky;
                        top: 0;
                        cursor: pointer;
                        user-select: none;
                    }
                    th:hover {
                        background-color: #45a049;
                    }
                    th::after {
                        content: ' ⇅';
                        font-size: 0.8em;
                        opacity: 0.5;
                    }
                    th.sort-asc::after {
                        content: ' ▲';
                        opacity: 1;
                    }
                    th.sort-desc::after {
                        content: ' ▼';
                        opacity: 1;
                    }
                    td {
                        padding: 10px;
                        border-bottom: 1px solid #ddd;
                    }
                    tr:hover {
                        background-color: #f5f5f5;
                    }
                    .amount-positive {
                        color: green;
                    }
                    .amount-negative {
                        color: red;
                    }
                    .nav-links {
                        margin-bottom: 20px;
                    }
                    .nav-links a {
                        margin-right: 15px;
                        color: #4CAF50;
                        text-decoration: none;
                    }
                    .nav-links a:hover {
                        text-decoration: underline;
                    }
                    .no-results {
                        text-align: center;
                        padding: 20px;
                        color: #666;
                    }
                </style>
            </head>
            <body>
                <h1>Akahu Transaction Log</h1>
                
                <div class="nav-links">
                    <a href="/">Home</a>
                    <a href="/status">Status</a>
                    <a href="/sync">Run Sync</a>
                    <a href="/transactions">Transaction Log</a>
                </div>
                
                <div class="summary">
                    <strong>Total Transactions:</strong> <span id="total-count">{{ total_count }}</span>
                    <span id="filtered-count" style="display:none;"> (Showing: <span id="visible-count">0</span>)</span>
                </div>
                
                {% if transactions %}
                <div class="filter-box">
                    <div class="filter-row">
                        <div>
                            <label for="account-filter">Filter by Account:</label>
                            <select id="account-filter">
                                <option value="">All Accounts</option>
                                {% for account in accounts %}
                                <option value="{{ account }}">{{ account }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div>
                            <label for="filter-input">Search Transactions:</label>
                            <input type="text" id="filter-input" placeholder="Search by date, payee, memo, or amount...">
                        </div>
                    </div>
                </div>
                
                <table id="transaction-table">
                    <thead>
                        <tr>
                            <th onclick="sortTable(0)" data-type="date">Date</th>
                            <th onclick="sortTable(1)" data-type="text">Account</th>
                            <th onclick="sortTable(2)" data-type="text">Payee</th>
                            <th onclick="sortTable(3)" data-type="text">Memo</th>
                            <th onclick="sortTable(4)" data-type="amount">Amount</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for txn in transactions %}
                        <tr>
                            <td>{{ txn.date }}</td>
                            <td>{{ txn.account }}</td>
                            <td>{{ txn.payee }}</td>
                            <td>{{ txn.memo }}</td>
                            <td>{{ txn.amount }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                <div id="no-results" class="no-results" style="display:none;">
                    No transactions match your filter.
                </div>
                {% else %}
                <p>No transactions found.</p>
                {% endif %}
                
                <script>
                    let sortDirection = {};
                    let currentSortColumn = -1;
                    
                    // Filter functionality
                    const filterInput = document.getElementById('filter-input');
                    const accountFilter = document.getElementById('account-filter');
                    const table = document.getElementById('transaction-table');
                    const noResults = document.getElementById('no-results');
                    const filteredCount = document.getElementById('filtered-count');
                    const visibleCount = document.getElementById('visible-count');
                    
                    function applyFilters() {
                        const textFilter = filterInput ? filterInput.value.toLowerCase() : '';
                        const selectedAccount = accountFilter ? accountFilter.value : '';
                        const rows = table.getElementsByTagName('tr');
                        let visibleRows = 0;
                        
                        for (let i = 1; i < rows.length; i++) {
                            const row = rows[i];
                            const cells = row.getElementsByTagName('td');
                            const account = cells[1].textContent; // Account is column 1
                            const rowText = row.textContent.toLowerCase();
                            
                            let showRow = true;
                            
                            // Apply account filter
                            if (selectedAccount && account !== selectedAccount) {
                                showRow = false;
                            }
                            
                            // Apply text filter
                            if (textFilter && !rowText.includes(textFilter)) {
                                showRow = false;
                            }
                            
                            if (showRow) {
                                row.style.display = '';
                                visibleRows++;
                            } else {
                                row.style.display = 'none';
                            }
                        }
                        
                        // Update display
                        const hasActiveFilter = textFilter !== '' || selectedAccount !== '';
                        
                        if (!hasActiveFilter) {
                            filteredCount.style.display = 'none';
                            noResults.style.display = 'none';
                            table.style.display = '';
                        } else {
                            visibleCount.textContent = visibleRows;
                            filteredCount.style.display = 'inline';
                            
                            if (visibleRows === 0) {
                                noResults.style.display = 'block';
                                table.style.display = 'none';
                            } else {
                                noResults.style.display = 'none';
                                table.style.display = '';
                            }
                        }
                    }
                    
                    if (filterInput) {
                        filterInput.addEventListener('keyup', applyFilters);
                    }
                    
                    if (accountFilter) {
                        accountFilter.addEventListener('change', applyFilters);
                    }
                    
                    // Sort functionality
                    function sortTable(columnIndex) {
                        const table = document.getElementById('transaction-table');
                        const tbody = table.getElementsByTagName('tbody')[0];
                        const rows = Array.from(tbody.getElementsByTagName('tr'));
                        const header = table.getElementsByTagName('th')[columnIndex];
                        const dataType = header.getAttribute('data-type');
                        
                        // Toggle sort direction
                        if (currentSortColumn === columnIndex) {
                            sortDirection[columnIndex] = !sortDirection[columnIndex];
                        } else {
                            sortDirection[columnIndex] = false; // Start with ascending
                            currentSortColumn = columnIndex;
                        }
                        
                        const isAscending = sortDirection[columnIndex];
                        
                        // Remove sort indicators from all headers
                        const headers = table.getElementsByTagName('th');
                        for (let i = 0; i < headers.length; i++) {
                            headers[i].classList.remove('sort-asc', 'sort-desc');
                        }
                        
                        // Add sort indicator to current header
                        header.classList.add(isAscending ? 'sort-asc' : 'sort-desc');
                        
                        // Sort rows
                        rows.sort((a, b) => {
                            const aValue = a.getElementsByTagName('td')[columnIndex].textContent.trim();
                            const bValue = b.getElementsByTagName('td')[columnIndex].textContent.trim();
                            
                            let comparison = 0;
                            
                            if (dataType === 'date') {
                                comparison = new Date(aValue) - new Date(bValue);
                            } else if (dataType === 'amount') {
                                const aNum = parseFloat(aValue.replace('$', '').replace(',', ''));
                                const bNum = parseFloat(bValue.replace('$', '').replace(',', ''));
                                comparison = aNum - bNum;
                            } else {
                                comparison = aValue.localeCompare(bValue);
                            }
                            
                            return isAscending ? comparison : -comparison;
                        });
                        
                        // Reorder the table
                        rows.forEach(row => tbody.appendChild(row));
                    }
                </script>
            </body>
            </html>
            """
            
            return render_template_string(
                html_template,
                transactions=transaction_list,
                total_count=len(transaction_list),
                accounts=unique_accounts
            )
            
        except Exception as e:
            logging.error(f"Error displaying transactions: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/receive-transaction", methods=["POST"])
    def receive_transaction():
        """Handle incoming webhook events from Akahu.
        Note: This endpoint is RFU (Reserved For Future Use) pending security audit and proper
        webhook authentication implementation."""
        signature = request.headers.get("X-Akahu-Signature")
        verify_signature(env_vars["AKAHU_PUBLIC_KEY"], signature, request.data)

        data = request.get_json()
        if data["type"] != "TRANSACTION_CREATED":
            return jsonify({"status": "ignored - not a transaction event"}), 200

        transactions = data["item"]
        akahu_account_id = transactions["account"]["_id"]
        mapping_entry = mapping_list[akahu_account_id]

        # Process for Actual Budget if enabled
        if RUN_SYNC_TO_AB and not mapping_entry.get("actual_do_not_map"):
            actual_client.download_budget()
            load_transactions_into_actual(
                pd.DataFrame([transactions]), mapping_entry, actual_client
            )

        # Process for YNAB if enabled
        if RUN_SYNC_TO_YNAB and not mapping_entry.get("ynab_do_not_map"):
            if mapping_entry.get("account_type") == "Tracking":
                # For tracking accounts, create balance adjustment
                akahu_balance = get_akahu_balance(
                    akahu_account_id,
                    env_vars["akahu_endpoint"],
                    env_vars["akahu_headers"],
                )
                if akahu_balance is not None:
                    akahu_balance_milliunits = int(akahu_balance * 1000)
                    ynab_balance_milliunits = get_ynab_balance(
                        mapping_entry["ynab_budget_id"],
                        mapping_entry["ynab_account_id"],
                    )
                    if ynab_balance_milliunits != akahu_balance_milliunits:
                        create_adjustment_txn_ynab(
                            mapping_entry["ynab_budget_id"],
                            mapping_entry["ynab_account_id"],
                            akahu_balance_milliunits,
                            ynab_balance_milliunits,
                            YNAB_ENDPOINT,
                            YNAB_HEADERS,
                        )
            else:
                # For regular accounts, process the transaction
                df = pd.DataFrame([transactions])
                cleaned_txn = clean_txn_for_ynab(df, mapping_entry["ynab_account_id"])
                load_transactions_into_ynab(
                    cleaned_txn,
                    mapping_entry["ynab_budget_id"],
                    mapping_entry["ynab_account_id"],
                    YNAB_ENDPOINT,
                    YNAB_HEADERS,
                )

        return jsonify({"status": "success"}), 200

    return app


def start_webhook_server(app, development_mode=False):
    """Start the Flask webhook server."""
    app.run(host="0.0.0.0", port=5000, debug=development_mode)
