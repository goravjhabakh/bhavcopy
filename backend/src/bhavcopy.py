import mysql.connector
import requests
from fake_useragent import UserAgent
from datetime import datetime,timedelta
import sys

class BhavCopy():
    def __init__(self,date):
        self.date = date
        self.ua = UserAgent()
        self.header = {'User-Agent':str(self.ua.chrome)}
        self.url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{self.date}.csv'
        print(self.url)

    def set_bhav(self):
        try:
            self.response = requests.get(self.url,headers=self.header)
            if self.response.status_code == 200:
                self.data = self.response.content.decode('utf-8').splitlines()
                self.data = [[col.strip() for col in row.split(',')] for row in self.data]
                print(f'Number of records in Input NSE Bhavcopy csv file : {len(self.data)}')
            else:
                sys.exit(0)
        except Exception as e:
            print('Error:',e)
        
        #return self.data

        try:
            self.data = self.data[1:]
            for i in range(len(self.data)):
                self.data[i][2] = datetime.strptime(self.data[i][2], '%d-%b-%Y').strftime('%Y-%m-%d')
                self.data[i].append(0)
                self.data[i].append(datetime.now().replace(microsecond=0))
            print(f'Modified Data\nHere is sample data:\n{self.data[0]}\nConnecting to database')
            self.connection = mysql.connector.connect(host='niveshsmartly.com',user='fdjtsuiv_trader',database='fdjtsuiv_trader',password='Tr@d3r$1234')
            self.cursor = self.connection.cursor() 
            print('Connected to database')
            self.cursor.execute('desc NSEBhavcopy')
            output = self.cursor.fetchall()
            self.columns = [col[0] for col in output[1:]]
            self.column_dict = self.label_encode(self.columns)
            #print(f'Bhavcopy table dict : {self.column_dict}')

            self.bhav_insert_query = f"INSERT INTO NSEBhavcopy ({','.join(self.columns)}) VALUES ({','.join(['%s'] * (len(self.columns)))})"
            self.select_query = ("Select AVG(AverageQty) AS AvgTradeQty, " + "AVG(DeliveryQty) AS AvgDeliveryQty, " + 
                                 "AVG(DeliveryPercent) AS AvgDelivPct " + "FROM (SELECT AverageQty, DeliveryQty, DeliveryPercent FROM NSEBhavcopy " + 
                                 "WHERE Symbol = %s order by Symbol, UploadDate desc LIMIT 10) subquery ")
            self.counter = 0
            self.EQ_counter = 0
            self.BE_counter = 0
            for row in self.data:
                self.symbol = row[self.column_dict['Symbol']]
                if row[self.column_dict['Series']] == 'EQ':
                    self.EQ_counter+=1
                elif row[self.column_dict['Series']] == 'BE':
                    row[self.column_dict['DeliveryPercent']] = 100
                    row[self.column_dict['DeliveryQty']] = row[self.column_dict['TotalTradeQty']]
                    self.BE_counter+=1
                if row[self.column_dict['Series']] in ('EQ','BE') :
                    try:
                        self.cursor.execute(self.select_query,(self.symbol,))
                        self.avg_data = self.cursor.fetchone()
                        self.deliveryqty = float(row[self.column_dict['DeliveryQty']] or 0)
                        self.deliverypercent = float(row[self.column_dict['DeliveryPercent']] or 0)
                        # if self.avg_data:
                        #     self.avg_trade_qty = float(self.avg_data[0] or 0)
                        #     self.avg_delivery_qty = float(self.avg_data[1] or 0)
                        #     self.avg_delivery_pct = float(self.avg_data[2] or 0)
                        #     self.avgqty = int(row[self.column_dict['TotalTradeQty']]) / int(row[self.column_dict['NoofTrades']])
                        #     self.delivqtychange = round((((self.deliveryqty * 100) / self.avg_delivery_qty) - 100),2)
                        #     self.delivpctchange = round((((self.deliverypercent * 100) / self.avg_delivery_pct) - 100),2)
                        #     self.tradepctchange = round((((self.avgqty * 100) / self.avg_trade_qty) - 100),2)
                        #     self.avgqty = round(self.avgqty,0)

                        if self.avg_data:
                            self.avg_trade_qty = float(self.avg_data[0] or 0)
                            self.avg_delivery_qty = float(self.avg_data[1] or 0)
                            self.avg_delivery_pct = float(self.avg_data[2] or 0)

                            try:
                                self.avgqty = int(row[self.column_dict['TotalTradeQty']]) / int(row[self.column_dict['NoofTrades']])
                            except ZeroDivisionError:
                                self.avgqty = 0

                            try:
                                self.delivqtychange = round((((self.deliveryqty * 100) / self.avg_delivery_qty) - 100), 2)
                            except ZeroDivisionError:
                                self.delivqtychange = 0

                            try:
                                self.delivpctchange = round((((self.deliverypercent * 100) / self.avg_delivery_pct) - 100), 2)
                            except ZeroDivisionError:
                                self.delivpctchange = 0

                            try:
                                self.tradepctchange = round((((self.avgqty * 100) / self.avg_trade_qty) - 100), 2)
                            except ZeroDivisionError:
                                self.tradepctchange = 0

                            self.avgqty = round(self.avgqty, 0)
                        else:
                            self.avg_trade_qty = 0
                            self.avg_delivery_qty = 0
                            self.avg_delivery_pct = 0
                            self.avgqty = 0
                            self.delivqtychange = 0
                            self.delivpctchange = 0
                            self.tradepctchange = 0
                        row.extend([self.avgqty, self.delivqtychange, self.delivpctchange, self.tradepctchange, 0, 0])
                        self.counter+=1
                        # print(f'Bhavcopy table columns : {self.columns}')
                        # print(f'Sample Data : {row}')
                        # print('Length of input data and table columns: '  , len(row), len(self.columns))
                        self.cursor.execute(self.bhav_insert_query,row)
                        if self.counter%50 == 0:
                            print(f'Inserted {self.counter+1} rows')
                        self.cursor.fetchall()
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        sys.exit(0)
            self.connection.commit()
            print(f'Number of Input BE Records: {self.BE_counter}')
            print(f'Number of Input EQ Records: {self.EQ_counter}')
            print(f'Number of records inserted into the table: {self.counter}')

            return self.data
        except Exception as e:
            print('Error:',e)
        

    def label_encode(self,row):
        d = {}
        x=0
        for element in row:
            if element not in d.keys():
                d[element] = x
                x+=1
        return d