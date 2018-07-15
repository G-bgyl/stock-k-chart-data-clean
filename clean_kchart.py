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

    def remove_useless_row(self, type = False):

        self.joe = self.joe.set_index('date')
        self.joe.index = pd.to_datetime(self.joe.index)
        # delete useless row that exceed the bound of time

        if type == 'SR':
            joe1 = self.joe.between_time('08:59:00', '15:00:59')
            joe2 = self.joe.between_time('20:59:00', '2:30:59')
            frames = [joe1, joe2]
            self.joe = pd.concat(frames)
            merge_begin0 = ' 08:59'
            merge_begin1 = ' 09:00'
            merge_late0 = ' 20:59'
            merge_late1 = ' 21:00'
        elif type == 'OP':
            self.joe = self.joe.between_time('09:29:00', '15:00:59')
            merge_begin0 = ' 09:29'
            merge_begin1 = ' 09:30'
            merge_late0 = None
            merge_late1 = None
        else:
            joe1 = self.joe.between_time('09:29:00', '15:00:59')
            joe2 = self.joe.between_time('20:59:00', '2:30:59')
            frames = [joe1, joe2]
            self.joe = pd.concat(frames)
            merge_begin0 = ' 09:29'
            merge_begin1 = ' 09:30'
            merge_late0 = ' 20:59'
            merge_late1 = ' 21:00'

        self.joe = self.joe[self.joe.buy1 != 0]
        self.joe = self.joe[self.joe.sale1 != 0]
        # self.joe = self.joe[self.joe.price != 0]

        return merge_begin0, merge_begin1, merge_late0, merge_late1

    def get_tstamp(self):
        try:

            # create minute column
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

    def get_data(self,each):
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

    def together_begin(self,early, minute_):
        open_, high_, low_, close_, volume_, opint_, bid1_, ask1_, bidvol1_, askvol1_ = early[1:]
        open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1 = minute_[1:]
        open_p = open_  # use early one
        high = max(high, high_)
        low = min(low, low_)
        volume = sum([volume_, volume])
        minute_[1:] = [open_p, high, low, close, volume, opint, bid1, ask1, bidvol1, askvol1]

    def Cal_k(self, path):

        #  iterate through each stock
        for name in self.stock_names:

            self.joe = self.df.loc[self.df['StockID'] == name]

            minutes = []
            type = name[:2]
            merge_begin0, merge_begin1, merge_late0, merge_late1 =self.remove_useless_row(type)

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

                        minute_ = []
                        minute_.append(str(key))
                        result = self.get_data(each)
                        minute_.extend(result)
                        if merge_begin0 in minute_[0]:
                            early = minute_
                        if merge_late0 and merge_late0 in minute_[0]:
                            late = minute_
                        minutes.append(minute_)

                    # write rows
                    for minute_ in minutes:

                        if merge_begin1 in minute_[0]:
                            # turn 8:59 and 9:00 into one
                            if early is not None:
                                self.together_begin(early,minute_)
                        if merge_late1 and merge_late1 in minute_[0]:
                            # turn 20:59 and 21:00 into one
                            if late is not None:
                                self.together_begin(late, minute_)
                        # ignore ' 08:59' and ' 20:59'
                        if merge_late0:
                            if merge_begin0 not in minute_[0] and merge_late0 not in minute_[0]:
                                # minute_[0]+=':00'
                                minute_[1:-2] = [round(num,4) for num in minute_[1:-2]]
                                writer.writerow(minute_)
                        else:
                            if merge_begin0 not in minute_[0]:
                                # minute_[0]+=':00'
                                minute_[1:-2] = [round(num,4) for num in minute_[1:-2]]
                                writer.writerow(minute_)

    def main(self):
        result_p = os.getcwd()+'/1w_output/'
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
    path = os.getcwd()+'/1w_data/'
    c = Clean_stock_data(path)
    c.main()
