# -*- coding: utf-8 -*-
#!/usr/bin/env python3
from __future__ import (absolute_import, division, print_function,unicode_literals)
import datetime
from datetime import datetime
import boto3
import json
import numpy as np
import pandas as pd
import os.path
import sys
import pytz
import time
from os.path import exists
import backtrader as bt
import ray

class TestSizer(bt.Sizer):
    params = (('stake', 1),)    
    def _getsizing(self, comminfo, cash, data, isbuy):        
        if isbuy:          
            return self.p.stake        
        position = self.broker.getposition(data)        
        if not position.size:            
            return 0        
        else:            
            return position.size        
        return self.p.stake

class TestStrategy(bt.Strategy):
    params = ( ('maperiod', 15),  ('printlog', False), )   
    def log(self, txt, dt=None, doprint=False):            
        dt = dt or self.datas[0].datetime.date(0)           
        print('%s, %s' % (dt.isoformat(), txt))    

    def __init__(self):        

        self.dataclose = self.datas[0].close      
        self.datahigh = self.datas[0].high        
        self.datalow = self.datas[0].low     

        self.order = None      
        self.buyprice = 0      
        self.buycomm = 0      
        self.newstake = 0      
        self.buytime = 0       
        # 参数计算，唐奇安通道上轨、唐奇安通道下轨、ATR        
        self.DonchianHi = bt.indicators.Highest(self.datahigh(-1), period=20, subplot=False)        
        self.DonchianLo = bt.indicators.Lowest(self.datalow(-1), period=10, subplot=False)       
        self.TR = bt.indicators.Max((self.datahigh(0)- self.datalow(0)), abs(self.dataclose(-1) -   self.datahigh(0)), abs(self.dataclose(-1)  - self.datalow(0) ))        
        self.ATR = bt.indicators.SimpleMovingAverage(self.TR, period=14, subplot=True)       
        # 唐奇安通道上轨突破、唐奇安通道下轨突破       
        self.CrossoverHi = bt.ind.CrossOver(self.dataclose(0), self.DonchianHi)        
        self.CrossoverLo = bt.ind.CrossOver(self.dataclose(0), self.DonchianLo)    
    def notify_order(self, order):        
        if order.status in [order.Submitted, order.Accepted]:            
            return

        if order.status in [order.Completed]:            
            if order.isbuy():               
                self.log(                    
                'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %                   
                (order.executed.price,
                order.executed.value,
                order.executed.comm),doprint=True)              
                self.buyprice = order.executed.price              
                self.buycomm = order.executed.comm            
            else:             
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %                        
                     (order.executed.price,
                     order.executed.value,
                     order.executed.comm),doprint=True)                             
                self.bar_executed = len(self)       
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:           
            self.log('Order Canceled/Margin/Rejected')        
        self.order = None    

    def notify_trade(self, trade):      
        if not trade.isclosed:
            return        
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' % (trade.pnl, trade.pnlcomm)) 

    def next(self): 
        if self.order:
            return        
        #入场        
        if self.CrossoverHi > 0 and self.buytime == 0:                                 
            self.newstake = self.broker.getvalue() * 0.01 / self.ATR            
            self.newstake = int(self.newstake / 100) * 100                             
            self.sizer.p.stake = self.newstake            
            self.buytime = 1            
            self.order = self.buy()        
        #加仓        
        elif self.datas[0].close >self.buyprice+0.5*self.ATR[0] and self.buytime > 0 and self.buytime < 5:           
            self.newstake = self.broker.getvalue() * 0.01 / self.ATR            
            self.newstake = int(self.newstake / 100) * 100            
            self.sizer.p.stake = self.newstake            
            self.order = self.buy()           
            self.buytime = self.buytime + 1        
        #出场        
        elif self.CrossoverLo < 0 and self.buytime > 0:            
            self.order = self.sell()            
            self.buytime = 0        
        #止损        
        elif self.datas[0].close < (self.buyprice - 2*self.ATR[0]) and self.buytime > 0:           
            self.order = self.sell()
            self.buytime = 0   
    def stop(self):        
        self.log('(MA Period %2d) Ending Value %.2f' % (15, self.broker.getvalue()), doprint=True)

def downloadFile(bucket_name, object_name, file_name):
    s3 = boto3.client('s3',region_name='ap-northeast-1')
    s3.download_file(bucket_name, object_name, file_name)
    print('succeed')

  
def uploadFile(file_name,bucket_name, key_name):
    s3 = boto3.client('s3',region_name='ap-northeast-1')
    s3.upload_file(file_name,bucket_name, key_name)

@ray.remote    
def BackTest(code,source_bucket_name,dest_bucket_name):
    cerebro = bt.Cerebro()
    # 增加一个策略
    cerebro.addstrategy(TestStrategy)

    #获取数据
    start_date = datetime(2010, 1, 1)  
    end_date = datetime(2022, 12, 30)  
    
    filename = code + '.csv'
    dest_filename = 'Result' + filename
    objectname = 'daily/' + filename
    downloadFile(source_bucket_name, objectname, filename)

    
    #os.chdir('/home/ec2-user/efs/workdir/industry/daily')
    stock_hfq_df = pd.read_csv(filename)
    stock_hfq_df.index = stock_hfq_df['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
    stock_hfq_df = stock_hfq_df[['open','close','high','low','volume']]
    
    data = bt.feeds.PandasData(dataname=stock_hfq_df, fromdate=start_date, todate=end_date)  # 加载数据
    cerebro.adddata(data)  # 将数据传入回测系统

    cerebro.broker.setcash(1000000.0)
    cerebro.broker.setcommission(commission=0.0002)# 设置交易手续费为 0.02%
    # 设置买入策略    
    cerebro.addsizer(TestSizer)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # 在结束时写入结果到S3存储桶
    f = open(dest_filename, "a")
    f.write('Final Portfolio Value: %.2f\n' % cerebro.broker.getvalue())
    f.write('Return: %.4f' % (float(cerebro.broker.getvalue())/1e6 - 1))
    f.close()
    targetobjectname = 'result/' + dest_filename
    uploadFile(dest_filename, dest_bucket_name, targetobjectname)


if __name__ == '__main__':

    bucket_name = 'quantbacktest'
    industry = 'bank'
    txtname = industry + '.txt'
    stock_list = []
    
    # 创建Ray集群
    ray.init()

    # 开始计时
    start_time = time.time()

    for line in open(txtname):  
        code = line[:6]
        stock_list.append(code)
        #filename = 'daily/' + code + '.csv'
        #stock_hfq_df = pd.read_csv(filename)
        #BackTest(code, bucket_name, bucket_name)
        #print(filename + 'Done')
    

    # 分布式回测
    backtest_tasks = [BackTest.remote(code, bucket_name, bucket_name) for code in stock_list]
    results = ray.get(backtest_tasks)

    # 结束计时
    end_time = time.time()  
    
    # 计算执行时间
    execution_time = end_time - start_time

    print(f"计算执行时间为: {execution_time}秒")

    # 关闭Ray集群
    ray.shutdown()
    sys.exit(0)

