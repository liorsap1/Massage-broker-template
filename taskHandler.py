#!/usr/bin/env python
import csv
import json

def writeTaskToCVS(dataToWrite, taksNumber, fieldsNames):
    csvFileName = 'results/task' + str(taksNumber) + '.csv'
    with open(csvFileName, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldsNames)
        writer.writeheader()
        for row in dataToWrite:
            writer.writerow({fieldsNames[0]: row[0], fieldsNames[1]: row[1]})

def task_1_query(conn):
    cur = conn.cursor()
    query = "SELECT BillingCountry, count(*) FROM invoices GROUP BY BillingCountry"
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def task1(conn):
    data = task_1_query(conn)
    writeTaskToCVS(data, 1, ['Country', 'Purchases'])


def task_2_query(conn):
    cur = conn.cursor()
    query = "SELECT BillingCountry, count(invoice_items.InvoiceLineId)  FROM invoices INNER JOIN invoice_items ON invoices.invoiceId = invoice_items.invoiceId GROUP BY BillingCountry"
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def task2(conn):
    data = task_2_query(conn)
    writeTaskToCVS(data, 2, ['Country', 'Items'])


def task_3_query(conn):
    cur = conn.cursor()
    innerQuery = "SELECT BillingCountry, invoice_items.InvoiceLineId, invoice_items.TrackId " \
                "FROM invoices INNER JOIN invoice_items ON invoices.invoiceId = invoice_items.invoiceId"
    query = "SELECT inv.BillingCountry, inv.TrackId, tracks.TrackId, tracks.AlbumId FROM ("+innerQuery+") AS inv LEFT JOIN tracks ON tracks.TrackId = inv.TrackId"
    outSideQuery = "SELECT invoice_tracks.BillingCountry AS Country, count(albums.AlbumId) AS Albums FROM (" + query + ") AS invoice_tracks INNER JOIN albums ON invoice_tracks.AlbumId = albums.AlbumId GROUP BY BillingCountry"
    cur.execute(outSideQuery)
    rows = cur.fetchall()
    return rows

def task3(conn):
    data = task_3_query(conn)
    taskJson = {}
    for row in data:
        taskJson.update({row[0]: row[1]})
    with open('results/task_3_country_albums.txt', 'w') as outfile:
        json.dump(taskJson, outfile)

def task5(conn):
    create_table_task1(conn)
    create_table_task2(conn)

def create_table_task1(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS country_invoices (Country, Purchases);")  # use your column names here
    with open('results/task1.csv', 'rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['Country'], i['Purchases']) for i in dr]
    cur.executemany("INSERT INTO country_invoices (Country, Purchases) VALUES (?, ?);", to_db)
    conn.commit()

def create_table_task2(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS country_invoices_items (Country, Items);")  # use your column names here
    with open('results/task2.csv', 'rb') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['Country'], i['Items']) for i in dr]
    cur.executemany("INSERT INTO country_invoices_items (Country, Items) VALUES (?, ?);", to_db)
    conn.commit()