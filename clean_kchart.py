import os
import pandas as pd
import re
import csv

class Clean_stock_data():
    def __init__(self):
        pass
    def get_df(self,path):
        # get file name
        file = [f for f in os.listdir(path) if re.search('(.+\.csv$)',f)]
        file = sorted(file)
        print('file name:', file)

        # get pandas dataframe
        df_dict = {}
        for i,each_file in enumerate(file):
            p = path+each_file
            df_dict[i] = pd.read_csv(p,encoding = "latin1")

        df_all = [df_dict[i] for i in range(len(df_dict))]
        self.df = pd.concat(df_all)

        return self.df.shape

    def get_stocks(self):
        # set the index to be this and don't drop
        self.df.set_index(keys=['StockID'], drop=False, inplace=True)
        # get a list of names
        self.stock_names=self.df['StockID'].unique().tolist()

    def remove_useless_row(self):
        # delete useless row that exceed the bound of time
        self.joe = self.joe.set_index('date')
        self.joe.index = pd.to_datetime(self.joe.index)
        joe1 = self.joe.between_time('09:29:00', '15:30:59')
        joe2 = self.joe.between_time('20:54:00', '2:30:59')
        frames = [joe1, joe2]

        self.joe = pd.concat(frames)

    def get_tstamp(self):
        # create minure column
        self.joe['minute'] = self.joe.index
        self.joe['minute'] = self.joe.minute.apply(str)
        self.joe['minute'] = self.joe['minute'].str.slice(stop=-3)
        grouped = self.joe.groupby('minute')
        return grouped

    def get_unique(self,result):
        if type(result) ==pd.core.series.Series:
            return result.unique()[0]
        else:
            return result

    def get_last(self,each):
        # cal
        open_p = each.iloc[0]['open']

        high = each.loc[each['high'].idxmax()]['high']
        high = self.get_unique(high)

        low = each.loc[each['low'].idxmin()]['low']
        low = self.get_unique(low)

        volume = each['vol'].sum()

        last = each.iloc[len(each.index) - 1]
        close = last['yclose']
        opint = last['sectional_cjbs']
        bid1 = last['buy1']
        ask1 = last['sale1']
        bidvol1 = last['bc1']
        askvol1 = last['sc1']

        return [open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1]
    def Cal_k(self,path):
        #  iterate through each stock
        for name in self.stock_names:
            with open('%s%s.csv'%(path,name),'w') as stock:
                writer = csv.writer(stock)
                title=['tstamp','open', 'high', 'low', 'close', 'volume', 'opint', 'bid1', 'ask1', 'bidvol1', 'askvol1']
                writer.writerow(title)
                self.joe = self.df.loc[self.df['StockID'] == name]

                minutes = []

                self.remove_useless_row()
                # get all stock names
                grouped = self.get_tstamp()
                early = None
                # loop through each minute
                for key,each in grouped :
                    minute_ = []
                    minute_.append(key)
                    result = self.get_last(each)
                    minute_.extend(result)
                    if '09:29' in minute_[0]:
                        early = minute_
                    minutes.append(minute_)

                # write rows
                for minute_ in minutes:

                    if '09:30' in minute_[0]:
                        # turn 9:29 and 9:30 into one
                        if early is not None:
                            open_, high_, low_, close_, volume_, opint_, bid1_, ask1_, bidvol1_, askvol1_ = early[1:]
                            open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1 = minute_[1:]
                            open_p = open_ #  use early one
                            high = max(high,high_)
                            low = min(low,low_)
                            volume = sum([volume_,volume])
                            minute_[1:] = [open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1]
                    if '09:29' not in minute_[0]:
                        writer.writerow(minute_)

    def main(self):
        path = './kchart_data/'
        # concat all the file together, then get shape of whole df
        shape = self.get_df(path)
        print(shape)
        # get index of different stocks
        self.get_stocks()
        # iterate through each name of stocks and output files.
        result_p='./kchart_output/'
        self.Cal_k(result_p)

if __name__ =='__main__':
    c = Clean_stock_data()
    c.main()
