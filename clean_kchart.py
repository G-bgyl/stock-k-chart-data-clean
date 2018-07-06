import os
import pandas as pd
import re
from datetime import datetime

class Clean_stock_data():
    def __init__(self):
        pass
    def get_df(self,path):

        file = [f for f in os.listdir(path) if re.search('(.+\.csv)',f)]


        file = sorted(file)
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
        self.joe.between_time('09:29:03', '15:00:59')

    def get_tstamp(self):
        t['minute'] = t.index
        t['minute'] = t.minute.apply(str)
        t['minute'] = t['minute'].str.slice(stop=-3)
        pass
    def get_open(self):
        pass
    def get_high(self):
        pass
    def get_low(self):
        pass
    def get_close(self):
        pass
    def get_volume(self):
        pass
    def get_opint(self):
        pass
    def get_bid1(self):
        pass
    def get_ask1(self):
        pass
    def get_bidvol1(self):
        pass
    def get_askvol1(self):
        pass
    def get_extreme(self,type=None):
        pass
    def Cal_k(self):
        #  iterate through each stock
        for name in self.stock_names:
            with open('%s.csv'%(name),'w') as stock:

                self.joe = self.df.loc[self.df['StockID'] == name]

                minute = []

                self.remove_useless_row()
                # get all names
                time = self.get_tstamp()

                for each in time :

                    # TODO: some of the function may be able to reuse
                    #
                    self.get_open() # first     1
                    self.get_high() # max       2
                    self.get_low()  # min       2
                    self.get_close()# last      1
                    self.get_volume() # sum     3
                    self.get_opint() # last     4
                    self.get_bid1() # last      4
                    self.get_ask1() # last      4
                    self.get_bidvol1() # last   4
                    self.get_askvol1() # last   4
                    minute.append('...')
                    stock.writerow(minute)

            # ....
    def main(self):
        path = './kchart_data/'
        # concat all the file together, then get shape of whole df
        shape = self.get_df(path)
        # get index of different stocks
        self.get_stocks()
        # iterate through each name of stocks and output files.
        self.Cal_k()

if __name__ =='__main__':
    c = Clean_stock_data()
    c.main()


