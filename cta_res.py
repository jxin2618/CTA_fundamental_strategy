# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 09:51:48 2020

@author: J Xin
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.cm as mplcm
import matplotlib.colors as colors
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
plt.rcParams['figure.figsize'] = (8.0, 4.0)


option = 'AL'

def view_results(file_list, title, idnum, condition):

    df = pd.DataFrame(data=[], columns=['date'])
    for file in file_list:
        sub_df = pd.read_excel(file)
        sub_df = sub_df.iloc[:,[2, -2]]
        df = pd.merge(df, sub_df, on='date', how='outer')
    df = df.fillna(method='ffill')
    col = df.shape[1]   
    x = df.loc[:, 'date']
    
    cm = plt.get_cmap('tab20')
    cNorm  = colors.Normalize(vmin=0, vmax=col-1)
    scalarMap = mplcm.ScalarMappable(norm=cNorm, cmap=cm)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    NUM_COLORS = col - 1

    # ax.set_prop_cycle([scalarMap.to_rgba(i) for i in range(col - 1)])
    
    for i in range(1, col):
        method = df.iloc[:, [i]].columns[0]
        method = method[11:-15]
        y = df.iloc[:, i].values
        lines = ax.plot(x, y, label=method)
        lines[0].set_color(cm(i/NUM_COLORS))
        # lines[0].set_color(clrs[i])
        
    res_path = save_path + idnum + '/' + condition
    os.chdir(res_path)
    plt.xlabel('date')
    plt.ylabel('net value')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2)
    plt.title(title)
    plt.grid()
    plt.tight_layout()
    plt.savefig('results.png', dpi=300, bbox_inches="tight")
    plt.show()

    print('got the figure')

    return


id_list = pd.read_excel('D:/Xin/Program/Quantamental/metal/钢联有色数据1.xlsx', sheet_name='卓创红期新版')
# id_list = id_list.iloc[-2:, :]

os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + option +  '/data/intermediate_data/')
save_path = 'D:/Xin/Program/Quantamental/metal/zc_new_data/' + option + '/figure/factors/'

file_chdir = os.getcwd()

# walk the directory and save the csv file name
file_list = []
for root, dirs, files in os.walk(file_chdir):
    if str(root)[-3:] == 'lag':
        file_list = []
        idnum = str.split(root, '\\')[-2]
        condition = str.split(root, '\\')[-1] 
        title = id_list.loc[id_list.loc[:, 'index_code'] == idnum, 'index_name'].values[0]
      
        os.chdir(root)
        for file in files:
            if os.path.splitext(file)[1] == '.xlsx' and len(os.path.splitext(file)[0]) > 10:
                file_list.append(file)
        
        view_results(file_list, title, idnum, condition)
    
    
    
    
    
    
    
    
    
    


