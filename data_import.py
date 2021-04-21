import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:jwl940615@localhost:3306/ey')
# order_type-  01：一线-不合规｜02：一线-未录入｜03：一线-正常｜04：一线-不发｜05：二线-正常｜06：二线-不发｜07：二线-不合规| 08：一线-不合规-不发
# data-set {雇佣主体,项目,主体,城市,成本中心,商圈ID,商圈,人员姓名,人员ID,本人身份证号码,手机号,入职日期,出勤天数,有效出勤,完成单量,应发服务费,实发服务费}

# path命名规则，named_month+order_id+no，如1_1_1,1_8_20注意根据自己的文件格式去调整代码
named_set = []
for named_month in range(1,13):
    for named_order_type in range(1,9):
        for no in range(1,20):
            a = str(named_month)+'-'+str(named_order_type)+'-'+str(no)
            named_set.append(a)
# print(named_set)

# 根据自身需要的字段去调整字段名
use_cols = ['雇佣主体','项目','主体','城市','成本中心','商圈ID','商圈','人员姓名','人员ID','本人身份证号码','手机号','入职日期','出勤天数','有效出勤','完成单量','应发服务费','实发服务费']

data_df = pd.DataFrame(columns= use_cols)
for a in named_set:
    try:
        sheet = pd.read_excel('./datasets/aaa/' + a + '.xlsx',usecols = use_cols)
        sheet['id'] = a
        data_df = data_df.append(sheet,ignore_index=True)
    except FileNotFoundError:
        continue

data_df['order_type'] = data_df['id'].str.split('-',expand = True)[1] #后续的统计需要用到order_type的，可以加上，也可以根据自己需求加上比如order_status等条件
data_df['month'] = data_df['id'].str.split('-',expand = True)[0] #原文档无时间，因此这边也需要加上时间
data_df.to_sql('combined_data',engine,index=True,if_exists='replace')