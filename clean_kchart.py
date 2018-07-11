import os
import pandas as pd
import re
import csv

class Clean_stock_data():
    def __init__(self,path):
        self.path = path


    def get_df(self,file):
        # get pandas dataframe
        df_dict = {}
        for i, each_file in enumerate(file):
            p = self.path + each_file
            df_dict[i] = pd.read_csv(p, encoding="latin1")

        df_all = [df_dict[i] for i in range(len(df_dict))]
        self.df = pd.concat(df_all)


    def get_fname(self):
        # get file name
        file = [f for f in os.listdir(self.path) if re.search('(.+\.csv$)',f)]
        file = sorted(file)

        # seperate files by day
        date = pd.DataFrame(file,columns=['fname'])
        date['date'] = date['fname'].str.slice(stop=-5)
        grouped = date.groupby('date')
        fnames = []
        for key, each in grouped:
            files = each['fname'].tolist()
            fnames.append(files)
        print('dir name:', fnames)

        return fnames

    def get_stocks(self):
        # set the index to be this and don't drop
        self.df.set_index(keys=['StockID'], drop=False, inplace=True)
        # get a list of names
        self.stock_names=self.df['StockID'].unique().tolist()

    def remove_useless_row(self, sr = False):


        # delete useless row that exceed the bound of time
        self.joe = self.joe.set_index('date')
        self.joe.index = pd.to_datetime(self.joe.index)
        if sr:
            joe1 = self.joe.between_time('08:59:00', '15:00:59')
            joe2 = self.joe.between_time('20:59:00', '2:30:59')
            frames = [joe1, joe2]
        else:
            joe1 = self.joe.between_time('08:59:00', '15:00:59')
            joe2 = self.joe.between_time('20:59:00', '2:30:59')
            frames = [joe1, joe2]

        self.joe = pd.concat(frames)
        self.joe = self.joe[self.joe.buy1 != 0]
        self.joe = self.joe[self.joe.sale1 != 0]
        # self.joe = self.joe[self.joe.price != 0]



    def get_tstamp(self):
        try:
            # create minure column
            self.joe['minute'] = self.joe.index
            self.joe['minute'] = self.joe.minute.apply(str)
            self.joe['minute'] = self.joe['minute'].str.slice(stop=-3)
            self.joe['minute'] = pd.to_datetime(self.joe['minute'])
            grouped = self.joe.groupby('minute')
            return grouped
        except:
            return None


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

    def Cal_k(self, path):

        #  iterate through each stock
        for name in self.stock_names:

            self.joe = self.df.loc[self.df['StockID'] == name]

            minutes = []


            # get all stock names
            grouped = self.get_tstamp()
            early = None
            late = None
            if grouped:

                with open('%s%s.csv' % (path, name), 'w') as stock:
                    writer = csv.writer(stock)
                    title = ['tstamp', 'open', 'high', 'low', 'close', 'volume', 'opint', 'bid1', 'ask1', 'bidvol1',
                             'askvol1']
                    writer.writerow(title)
                    # loop through each minute
                    for key,each in grouped :
                        self. joe = each
                        self.remove_useless_row()
                        minute_ = []
                        minute_.append(key)
                        result = self.get_last(each)
                        minute_.extend(result)
                        if ' 08:59' in minute_[0]:
                            early = minute_
                        if ' 20:59' in minute_[0]:
                            late = minute_
                        minutes.append(minute_)

                    # write rows
                    for minute_ in minutes:

                        if ' 09:00' in minute_[0]:
                            # turn 8:59 and 9:00 into one
                            if early is not None:
                                open_, high_, low_, close_, volume_, opint_, bid1_, ask1_, bidvol1_, askvol1_ = early[1:]
                                open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1 = minute_[1:]
                                open_p = open_ #  use early one
                                high = max(high,high_)
                                low = min(low,low_)
                                volume = sum([volume_,volume])
                                minute_[1:] = [open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1]
                        if ' 21:00' in minute_[0]:
                            # turn 20:59 and 21:00 into one
                            if late is not None:
                                open_, high_, low_, close_, volume_, opint_, bid1_, ask1_, bidvol1_, askvol1_ = late[1:]
                                open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1 = minute_[1:]
                                open_p = open_  # use early one
                                high = max(high, high_)
                                low = min(low, low_)
                                volume = sum([volume_, volume])
                                minute_[1:] = [open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1]
                        # ignore ' 08:59' and ' 20:59'
                        if ' 08:59' not in minute_[0] and ' 20:59' not in minute_[0]:
                            # minute_[0]+=':00'
                            minute_[1:-2] = [round(num,4) for num in minute_[1:-2]]
                            writer.writerow(minute_)

    def main(self):
        result_p = os.getcwd()+'/kchart_output/1w/'
        # concat all the file together
        dir_names = self.get_fname()

        for names in dir_names:
            dir_name = names[0][:-6]
            # p = os.getcwd()
            date_path = '%s%s/'%(result_p,dir_name)
            if not os.path.exists(date_path):
                os.mkdir(date_path, 0o777)
            self.get_df(names)
            # get index of different stocks
            self.get_stocks()
            # iterate through each name of stocks and output files.
            self.Cal_k(date_path)

if __name__ =='__main__':
    path = os.getcwd()+'/kchart_data/clean_1w/'
    c = Clean_stock_data(path)
    c.main()
