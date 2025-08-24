import mysql.connector
from mysql.connector import Error
import logging
import configparser
import csv
import time
import tkinter as tk
from tkinter import ttk
from threading import Thread
from datetime import datetime
import datetime
from decimal import Decimal

# Setup logging
logging.basicConfig(level=logging.ERROR, filename='app.log', filemode='w')

# Read configuration
config = configparser.ConfigParser()
config.read(r'/indus.ini')

# File name for CSV output
fname = config.get('CSV', 'filename')

# Flags to control the data transfer loop
transfer_running = False

# Function to get the current timestamp
def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Function to change the color of the connection status indicator
def update_indicator(canvas, color):
    canvas.itemconfig('status', fill=color)

# Function to print messages to the log text widget
def log_message(text_widget, message):
    text_widget.config(state=tk.NORMAL)
    text_widget.insert(tk.END, message + "\n")
    text_widget.config(state=tk.DISABLED)
    text_widget.see(tk.END)  # Auto-scroll to the bottom

# Function to connect to MariaDB and transfer data from DB1
def db1_to_mariadb(status_label, canvas_db1, text_widget, progress_bar):
    global transfer_running

    while transfer_running:
        try:
            # Connect to MariaDB
            user = config.get('MySqlTest', 'username')
            pwd = config.get('MySqlTest', 'password')
            db = config.get('MySqlTest', 'dbname')
            host = config.get('MySqlTest', 'host')
            port = config.get('MySqlTest', 'port')

            log_message(text_widget, "Connecting to MariaDB...")
            connection = mysql.connector.connect(host=host, database=db, user=user, password=pwd)
            if connection.is_connected():
                cur_mariadb = connection.cursor()
                status_label.config(text=f"Connected to MariaDB from {db}")
                update_indicator(canvas_db1, "green")

                # Fetch data from MariaDB
                # where status in ('MAT-RECO-2024-00033','MAT-RECO-2024-00037')

                cur_mariadb.execute(f"""
                        select distinct t2.item_code ,t2.warehouse
                        from `tabStock Ledger Entry` t2
                        where 	t2.is_cancelled=0 and
                                t2.posting_date>='2024-07-01' and
                                t2.voucher_type='Purchase Receipt' and
                                t2.incoming_rate<=1
			            and
					    (t2.item_code ,t2.warehouse)  in ( select item_code, warehouse from stock_050124  )
                        """)

                item_rows = cur_mariadb.fetchall()
                log_message(text_widget, f"Fetched {len(item_rows)} rows from Item table ")

                 # Set progress bar maximum value
                total_rows = len(item_rows)
                progress_bar.config(maximum=total_rows)
                processed_rows = 0

                # Process each item and update progress bar
                for index, item in enumerate(item_rows, start=1):
                    if not transfer_running:  # Check if the transfer is stopped
                        log_message(text_widget, "Transfer stopped manually. Exiting loop.")
                        break
                    # [Data Processing Code Here]
                    
                    processed_rows += 1
                    progress_bar['value'] = processed_rows
                    log_message(text_widget, f"Processing item {processed_rows}/{total_rows}")
                    status_label.config(text=f"Processing row {processed_rows}/{total_rows}: item:{item[0]}, Warehouse:{item[1]}")
                    
                    # # Write query for back row of selected item 
                    
                    query = f"""
                                SELECT 
                                    MAX(name) AS max_name, 
                                    qty_after_transaction, 
                                    valuation_rate, 
                                    stock_value,
                                    STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s') AS dttime
                                FROM `tabStock Ledger Entry`
                                WHERE 
                                    is_cancelled = 0 and
                                    item_code = '{item[0]}' and warehouse='{item[1]}'
                                    and STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s') = 
                                    (
                                        SELECT 
                                            MAX(STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s')) 
                                        FROM `tabStock Ledger Entry`
                                        WHERE 
                                            is_cancelled = 0 and
                                            posting_date<'2024-07-01' and
                                            item_code = '{item[0]}' and warehouse='{item[1]}'
                                    )
                                LIMIT 1;
                            """
                    # log_message(text_widget, f" {query}")
                    cur_mariadb.execute(query)

                    op_rows = cur_mariadb.fetchone()
                    log_message(text_widget, f"op rows {op_rows} ")

              
                    # opening variable
                    if op_rows:
                        op_ref = f"'{op_rows[0]}'" if op_rows[0] else 'NULL'  # Ensure NULL if no value
                        op_qty = op_rows[1] if op_rows[1] is not None else 0
                        op_rate = op_rows[2] if op_rows[2] is not None else 0
                        op_amount = op_rows[3] if op_rows[3] is not None else 0
                        dttime = f"'{op_rows[4]}'" if op_rows[4] else 'NULL'

                        bf_qty = op_qty
                        last_avg_rate = op_rate
                        last_value = op_amount
                    else:
                        op_ref = 'NULL'
                        op_qty = 0
                        op_rate = 0
                        op_amount = 0
                        dttime = 'NULL'
                        last_avg_rate = 0
                        last_value = 0
                        bf_qty = 0
                    
                    bf_qty= 0 if bf_qty is None else bf_qty
                    last_value= 0 if bf_qty is None else last_value
                        
                    # log_message(text_widget, f"purchase_amount={tot_purchase_amt}, purchase_qty={tot_purchase_qty}, avg={avg_purchase_rate}, stock_qty={op_qty},rate={op_rate},stock_value={op_amount} ")
                    rows = fetch_ordered_stock_rows(cur_mariadb, item[0],item[1], '2024-07-01')
                    log_message(text_widget, f"Fetched {len(rows)} rows from stock table of item {item[0]}, {item[1]} ")

                    # progress_bar.config(maximum=total_rows)
                    item_processed_rows = 0
                    item_total_rows = len(rows)

                    if rows:
                         for row in rows:
                            log_message(text_widget, f"current row {row} ")

                            if not transfer_running:  # Check if the transfer is stopped
                                log_message(text_widget, "Transfer stopped manually. Exiting loop.")
                                break

                            item_processed_rows += 1
                            # log_message(text_widget, f"current {row} ")
                            status_label.config(text=f"Processing row {processed_rows}/{total_rows}: item:{item[0]},Warehouse:{item[1]}, Date:{row[3]} - {item_processed_rows}/{item_total_rows}")

                            # avg_rate = round( (last_value + row[17]) / (bf_qty + row[11]) ,4) if row[7] in ('Purchase Receipt','Received from Subcontractor') and row[13] > 1 else last_avg_rate
                            # avg_rate = row[15] if avg_rate==0 else avg_rate

                            # v_stock_value_difference=round(row[11]*avg_rate,4)
                            # v_amount=abs(round(row[11]*avg_rate,4))
                            # cf_value=last_value+v_stock_value_difference
                            # cf_qty = bf_qty + row[11]
                            # log_message(text_widget, f"{row[7]} ,{row[0]} last rate= {last_avg_rate} , avg_rate{avg_rate}, bf={bf_qty},qty={row[11]},  cf_qty={cf_qty} cf_value={cf_value},")

                            # Update MariaDB row rate and balance
                            try:
                                   
                                if row[7] in ('Purchase Receipt','Received from Subcontractor'):
                                    avg_rate = round( (last_value + row[17]) / (bf_qty + row[11]) ,4) if row[13] > 0 else last_avg_rate
                                    avg_rate = row[15] if avg_rate==0 else avg_rate
                                    v_stock_value_difference=round(row[11]*avg_rate,4)
                                    v_amount=abs(round(row[11]*avg_rate,4))
                                    cf_value=last_value+v_stock_value_difference
                                    cf_qty =bf_qty + row[11]
                                    log_message(text_widget, f"{row[7]} ,{row[0]} last rate= {last_avg_rate} , avg_rate{avg_rate}, bf={bf_qty},qty={row[11]},  cf_qty={cf_qty} cf_value={cf_value},")


                                    SQL_UPDATE_MYSQL = f"""UPDATE `tabStock Ledger Entry` set 
                                                            qty_after_transaction={cf_qty},
                                                            stock_value={cf_value},
                                                            valuation_rate={avg_rate},
                                                            stock_value_difference=round(actual_qty*incoming_rate,4),
                                                            stock_queue=null
                                                            WHERE name = %s
                                                        """
                                    # log_message(text_widget, f" {SQL_UPDATE_MYSQL} ")
                                    bf_qty=cf_qty
                                    last_avg_rate = avg_rate
                                    last_value+=round(row[11]*row[13],4)
          
                                elif row[7] in ('Stock Reconciliation'):
                                    SQL_UPDATE_MYSQL = f"""UPDATE `tabStock Ledger Entry` set 
                                                            stock_value_difference= {last_value}-{row[16]},
                                                            stock_queue=null
                                                            WHERE name = %s
                                                        """


                                    bf_qty=row[12]
                                    last_avg_rate = row[15]
                                    last_value=row[16]
                                    log_message(text_widget, f" Stock Reconsilation {row[7]},last rate= {last_avg_rate}  , bf={bf_qty}, last value={last_value}")
                                else:

                                    avg_rate = last_avg_rate
                                    # avg_rate = round( (last_value + row[17]) / (bf_qty + row[11]) ,4) if row[11]>0  else last_avg_rate
                                    avg_rate = row[15] if avg_rate==0 else avg_rate
                                    v_stock_value_difference=round(row[11]*avg_rate,4) if row[11]<0 else row[17]
                                    v_amount=abs(round(row[11]*avg_rate,4))
                                    cf_value=last_value+v_stock_value_difference
                                    cf_qty = bf_qty + row[11]
                                    log_message(text_widget, f"{row[7]} ,{row[0]} last rate= {last_avg_rate} , avg_rate{avg_rate}, bf={bf_qty},qty={row[11]},  cf_qty={cf_qty} cf_value={cf_value},")

                                    SQL_UPDATE_MYSQL = f"""UPDATE `tabStock Ledger Entry` set 
                                                            valuation_rate={avg_rate},
                                                            qty_after_transaction={cf_qty},
                                                            stock_value={cf_value},
                                                            stock_value_difference={v_stock_value_difference} ,
                                                            incoming_rate=case when actual_qty>0 then incoming_rate else 0 end,
                                                            outgoing_rate=case when actual_qty<0 then {avg_rate} else 0 end,
                                                            stock_queue=null
                                                            WHERE name = %s
                                                        """
                                    # log_message(text_widget, f" {SQL_UPDATE_MYSQL} ")
                                    last_avg_rate = avg_rate
                                    last_value=cf_value
                                    bf_qty=cf_qty

                                try:
                                    cur_mariadb.execute(SQL_UPDATE_MYSQL, (row[0],))
                                    connection.commit()
                                    log_message(text_widget, f"Updated Success {row[7]} for:{row[0]} ")

                                except Exception as e:
                                    log_message(text_widget, f"Updated error MariaDB row status for:{row[0]} ")



                                if row[7] in ('Received from Subcontractor'):
                                    try:
                                        SQL_UPDATE_Stock_entry = f"""UPDATE `tabStock Entry Detail` set valuation_rate={avg_rate}
                                                                        WHERE name ='{row[9]}' and item_code='{item[0]}'
                                                                """
                                        log_message(text_widget, f" {SQL_UPDATE_Stock_entry}")
                                        cur_mariadb.execute(SQL_UPDATE_Stock_entry)
                                        connection.commit()
                                        log_message(text_widget, f"Updated {row[7]} for:{row[0]} - {row[9]}")
                                    except Exception as e:
                                        
                                        log_message(text_widget, f"Updated error stock entry row status for:{row[0]} - {row[9]}")
                                else:
                                    
                                    try:
                                        SQL_UPDATE_Stock_entry = f"""UPDATE `tabStock Entry Detail` set basic_rate = {avg_rate},valuation_rate={avg_rate},
                                                                         basic_amount=round(qty*{avg_rate},4),amount=round(qty*{avg_rate},4)
                                                                        WHERE name ='{row[9]}' and item_code='{item[0]}' and s_warehouse='{item[1]}'
                                                                """
                                        # log_message(text_widget, f" {SQL_UPDATE_Stock_entry}")
                                        cur_mariadb.execute(SQL_UPDATE_Stock_entry)
                                        connection.commit()
                                        log_message(text_widget, f"Updated  {row[7]} ,stock entry detail for:{row[0]} - {row[9]}")
                                    except Exception as e:
                                        
                                        log_message(text_widget, f"Updated error stock entry row status for:{row[0]} - {row[9]}")
                                    
                                    try:
                                        SQL_UPDATE_MYSQL = f"""UPDATE `tabStock Ledger Entry` set 
                                                            valuation_rate={avg_rate},
                                                            stock_value=round(qty_after_transaction*{avg_rate},4),
                                                            stock_value_difference=round(actual_qty*{avg_rate} ,4),
                                                            incoming_rate=case when actual_qty>0 then {avg_rate} else 0 end,
                                                            outgoing_rate=case when actual_qty<0 then {avg_rate} else 0 end,
                                                            stock_queue=null
                                                            WHERE voucher_type='Stock Entry' and voucher_detail_no='{row[9]}' and item_code='{item[0]}' and warehouse<>'{item[1]}'
                                                            and actual_qty>0
                                                        """
                                        # log_message(text_widget, f" {SQL_UPDATE_MYSQL}")
                                        cur_mariadb.execute(SQL_UPDATE_MYSQL)
                                        connection.commit()
                                        log_message(text_widget, f"Updated {row[7]} ,stock ledger for:{row[0]} - {row[9]}")
                                    except Exception as e:

                                        log_message(text_widget, f"Updated error stock ledger row status for:{row[0]} - {row[9]}")



                                # with open(fname, "a", newline='') as csv_file:
                                #     csv_writer = csv.writer(csv_file)
                                #     csv_writer.writerow( list(row) + [db, host])
                                #     log_message(text_widget, f"Row written to CSV")
                            except Exception as e:
                                status_label.config(text=f"Unexpected Error: {e}")
                                update_indicator(canvas_db1, "red")
                                # transfer_running = False
                                # log_message(text_widget, "Transfer stopped due to error. Exiting loop.")
                                # break

                    

                    query = f"""
                                SELECT 
                                    MAX(name) AS max_name, 
                                    qty_after_transaction, 
                                    valuation_rate, 
                                    stock_value,
                                    STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s') AS dttime
                                FROM `tabStock Ledger Entry`
                                WHERE 
                                    is_cancelled = 0 and
                                    item_code = '{item[0]}' and warehouse='{item[1]}'
                                    and STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s') = 
                                    (
                                        SELECT 
                                            MAX(STR_TO_DATE(CONCAT(posting_date, ' ', posting_time), '%Y-%m-%d %H:%i:%s')) 
                                        FROM `tabStock Ledger Entry`
                                        WHERE 
                                            is_cancelled = 0 and
                                            item_code = '{item[0]}' and warehouse='{item[1]}'
                                    )
                                LIMIT 1;
                            """
                    # log_message(text_widget, f" {query}")
                    cur_mariadb.execute(query)

                    last_rows = cur_mariadb.fetchone()
                    log_message(text_widget, f"Last rows {last_rows} ")

                    if last_rows:
                        stk_ref = f"'{last_rows[0]}'" if last_rows[0] else 'NULL'  # Ensure NULL if no value
                        stk_qty = last_rows[1] if last_rows[1] is not None else 0
                        stk_rate = last_rows[2] if last_rows[2] is not None else 0
                        stk_amount = last_rows[3] if last_rows[3] is not None else 0
                        stkk_dttime = f"'{last_rows[4]}'" if last_rows[4] else 'NULL'
                    else:
                        stk_ref = 'NULL'
                        stk_qty = 0
                        stk_rate = 0
                        stk_amount = 0
                        stkk_dttime = 'NULL'


                    # purchse variable
                    # # Write query for back row of selected item 
                    
                    cur_mariadb.execute(f"""
                                SELECT sum(actual_qty) as tot_qty,sum(stock_value_difference) as tot_amount,
												round( COALESCE(sum(stock_value_difference),0)/COALESCE( sum(actual_qty),0) , 4) avg_rate
                                FROM `tabStock Ledger Entry` 
                                WHERE   is_cancelled=0 and 
                                    item_code = '{item[0]}' and warehouse='{item[1]}'
                                    and posting_date <='2025-01-05'
                                    and voucher_type='Purchase Receipt'
                                ;		
                                """)
                    tot_purchase_row = cur_mariadb.fetchone()
                    log_message(text_widget, f"total purchase row {tot_purchase_row} ")

                    if tot_purchase_row:
                        tot_purchase_qty = tot_purchase_row[0] if tot_purchase_row[0] is not None else 0
                        tot_purchase_amt = tot_purchase_row[1] if tot_purchase_row[1] is not None else 0
                        avg_purchase_rate = tot_purchase_row[2] if tot_purchase_row[2] is not None else 0
                    else:
                        tot_purchase_qty = 0
                        tot_purchase_amt = 0
                        avg_purchase_rate = 0


                    # purchse variable
                    # # Write query for back row of selected item 
                            
                    cur_mariadb.execute(f"""
                            SELECT sum(actual_qty) as tot_qty,sum(stock_value_difference) as tot_amount,
                                                        round( COALESCE(sum(stock_value_difference),0)/COALESCE( sum(actual_qty),0) , 4) avg_rate
                                FROM `tabStock Ledger Entry` 
                                WHERE   is_cancelled=0 and 
                                        item_code = '{item[0]}' and warehouse='{item[1]}'
                                        and posting_date <='2025-01-05'
                                        and voucher_type='Stock Entry'
                                        and actual_qty>0
                                        ;		
                            """)
                    tot_received_row = cur_mariadb.fetchone()
                    log_message(text_widget, f"total purchase row {tot_received_row} ")

                    if tot_received_row:
                        tot_received_qty = tot_received_row[0] if tot_received_row[0] is not None else 0
                        tot_received_amt = tot_received_row[1] if tot_received_row[1] is not None else 0
                        tot_received_rate = tot_received_row[2] if tot_received_row[2] is not None else 0
                    else:
                        tot_received_qty = 0
                        tot_received_amt = 0
                        tot_received_rate = 0
                        
                    log_message(text_widget, f"purchase_amount={tot_purchase_amt}, purchase_qty={tot_purchase_qty}, avg={avg_purchase_rate}, stock_qty={op_qty},rate={op_rate},stock_value={op_amount} ")
                    # try:
                    #     SQL_UPDATE_MYSQL = f"""
                    #         insert into `stock_050124` (item_code,warehouse,op_ref,op_date,op_qty,op_rate,op_amount,purchase_qty,purchase_amount,avg_purchase_rate,trf_incoming_qty,trf_incoming_amount,trf_incoming_avg_rate,stock_qty,valuation_rate,stock_value)
                    #          values ('{item[0]}','{item[1]}',  {op_ref}, {dttime},{op_qty}, {op_rate}, {op_amount},{tot_purchase_qty},{tot_purchase_amt},{avg_purchase_rate},{tot_received_qty},{tot_received_amt},{tot_received_rate},{stk_qty},{stk_rate},{stk_amount} )
                    #         """
                    #     log_message(text_widget, f" {SQL_UPDATE_MYSQL}")
                    #     cur_mariadb.execute(SQL_UPDATE_MYSQL)
                    #     connection.commit()
                    #     # log_message(text_widget, f"Updated MariaDB row status for: {item[0]}")


                    # except Exception as e:
                    #     log_message(text_widget, f"Error updating row {item[0]}: {e}")
                    #     log_message(text_widget, f"Failed Query: {SQL_UPDATE_MYSQL}")
                    #     status_label.config(text=f"Unexpected Error: {e}")
                    #     update_indicator(canvas_db1, "red")

                    try:
                        SQL_UPDATE_bin = f"""UPDATE `tabBin` set 
                                                actual_qty={stk_qty},
                                                valuation_rate={stk_rate},
                                                stock_value={stk_amount}
                                            WHERE item_code='{item[0]}' and warehouse ='{item[1]}'
                                            """
                        # log_message(text_widget, f" {SQL_UPDATE_Stock_entry}")
                        cur_mariadb.execute(SQL_UPDATE_bin)
                        connection.commit()
                        # log_message(text_widget, f"Updated MariaDB row status for: for:{row[0]} - {row[9]}")
                    except Exception as e:                           
                        log_message(text_widget, f"Updated error MariaDB row status for:{row[0]} - {row[9]}")


                    # Update progress bar and log progress
                    progress_bar["value"] = index
                    progress_bar.update()
                    # log_message(text_widget, f"Processing item {index}/{total_items}: {item[0]}")

                transfer_running = False

        except mysql.connector.Error as e:
            log_message(text_widget, f"MariaDB Error: {e}")
            status_label.config(text=f"MariaDB Error: {e}")
            update_indicator(canvas_db1, "red")

        except Exception as e:
            log_message(text_widget, f"Unexpected Error: {e}")
            status_label.config(text=f"Unexpected Error: {e}")
            update_indicator(canvas_db1, "red")

        finally:
            # Close connections
            if connection.is_connected():
                cur_mariadb.close()
                connection.close()
                log_message(text_widget, "MariaDB connection closed")
                update_indicator(canvas_db1, "red")


def fetch_ordered_stock_rows(cur_mariadb, item_code, wh,start_date):
    # Fetch rows from stock ledger ordered by posting date and time
    sql_stock_ledger = f"""
        SELECT
            name,
            creation,
            ROW_NUMBER() OVER(ORDER BY posting_date, posting_time) AS sno,
            posting_date,
            posting_time,
            item_code,
            warehouse,
            case when voucher_type='Stock Entry' then (select stock_entry_type from `tabStock Entry` where name=voucher_no) else voucher_type end as voucher_type,
            voucher_no,
            voucher_detail_no,
            recalculate_rate,
            actual_qty,
            qty_after_transaction,
            ROUND(incoming_rate, 4),
            ROUND(outgoing_rate, 4),
            case when voucher_type='Stock Entry' and voucher_detail_no is not null and actual_qty>0  then 
                # ( select max(basic_rate) from `tabStock Entry Detail` as rt where rt.name=stk.voucher_detail_no and item_code='{item_code}'  and valuation_rate>0 ) 
                ( select max(valuation_rate) from `tabStock Ledger Entry` rt where rt.voucher_no=stk.voucher_no and item_code='{item_code}' and valuation_rate>0 and warehouse!='{wh}')
            else 
                valuation_rate
            end as valuation_rate,
            stock_value,
            stock_value_difference
        FROM 
            `tabStock Ledger Entry` as stk
        WHERE is_cancelled = 0 
            AND item_code = '{item_code}' 
            AND warehouse = '{wh}' 
            AND posting_date >= '{start_date}'
        ORDER BY 
            posting_date, posting_time, creation;
    """
    cur_mariadb.execute(sql_stock_ledger)
    return cur_mariadb.fetchall()



def start_transfer(status_label, canvas_db1, text_widget, progress_bar):
    global transfer_running
    transfer_running = True
    status_label.config(text="Starting data update...")
    log_message(text_widget, "Data update started")

    # Start mysql db threads
    db1_thread = Thread(target=db1_to_mariadb, args=(status_label, canvas_db1, text_widget, progress_bar))
    db1_thread.start()

def stop_transfer(status_label, text_widget):
    global transfer_running
    transfer_running = False
    status_label.config(text="Stopping data update...")
    log_message(text_widget, "Data update stopped")

# Build the UI
def build_ui():
    window = tk.Tk()
    window.title("MariaDB Data Update")

    # Status label
    status_label = tk.Label(window, text="Status: Not started", width=100, height=2)
    status_label.pack()

    # Log output (Text widget)
    log_text = tk.Text(window, width=200, height=20, state=tk.DISABLED)
    log_text.pack()

    # Scrollbar for log_text
    scrollbar = tk.Scrollbar(window)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    log_text.config(yscrollcommand=scrollbar.set)
    scrollbar.config(command=log_text.yview)

    # Start button
    start_button = tk.Button(window, text="Start Transfer", command=lambda: start_transfer(status_label, canvas_db1, log_text, progress_bar))
    start_button.pack()

    # Stop button
    stop_button = tk.Button(window, text="Stop Transfer", command=lambda: stop_transfer(status_label, log_text))
    stop_button.pack()

    # Progress bar
    progress_bar = ttk.Progressbar(window, length=300, mode='determinate')
    progress_bar.pack(pady=10)

    # DB1 Connection Indicator
    canvas_db1 = tk.Canvas(window, width=50, height=50)
    canvas_db1.create_oval(10, 10, 40, 40, fill="red", tags='status')
    canvas_db1.pack()
    tk.Label(window, text=f"MysqlDB Connection:").pack()

    # Start the GUI loop
    window.mainloop()

# Run the UI
if __name__ == "__main__":
    build_ui()
