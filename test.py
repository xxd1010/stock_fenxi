import baostock as bs

lg = bs.login(user_id="anonymous", password="123456")
print('login respond error_code:', lg.error_code) # 为'0'则表示成功[citation:9]
print('login respond error_msg:', lg.error_msg)
rs = bs.query_history_k_data_plus("sh.600001",
                                  "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                                  start_date='2025-01-01', end_date='2025-01-31', frequency="d", adjustflag="2")
bs.logout()
print(rs.get_data())
