#! /usr/bin/env python3
import csv
import re
import sys
import warnings
warnings.filterwarnings("ignore")

#file = open("1000row.csv","r")s
#reader = csv.reader(file)
#df = pd.DataFrame()

reader = csv.reader(sys.stdin)

#删去的变量
drop = [0,2,3,4,5,6,7,8,9,11,12,13,15,16,17,20,24,26,28,29,30,31,33,34,36,37,38,39,40,41,42,45,46,47,48,50,52,53,54,55,56,57,58,59,60,61,62]

#删除的自变量
# 0 vin 2 bed  3 bed_height  4 bed_length 6 cabin 7 city 
# 8 city_fuel_economy  9 combine_fuel_economy 11 dealer_zip  12 description 
# 15 engine_type 16 exterior_color 17 fleet 20 franchise_make 26 high_way_economy 28 interior_color
# 30 is_certified 31 is_cpo 33 is_oemcpo 34 latitude  39 longitude
# 36 listed_date  37 listing_color  38 listing_id 
# 40 main_picture_url  41 major_options 42 make_name 45 model_name 46 owner_count
# 50 savings_amount 52 sp_id 53 sp_name 54 theft_title
# 57 transmission_display 58 trimid 59 trim_name
# 60 vehicle_damage_category 62 wheel_system_display

#因变量
# 48 price

#拆分成两个自变量
#47 power   177   hp @ 5,750 RPM   
#55 torque    200   lb-ft @ 1,750 RPM

#保留的自变量
# 1 back_legroom 5 body_type 10 daysonmarket 
# 13 engine_cylinders 14 engine_displacement 
# 18 frame_damaged 19 franchise_dealer 
# 21 front_legroom 22 fuel_tank_volume 23 fuel_type 24 has_accidents
# 25 height 27 horsepower 29 isCab 32 is_new 35 length 
# 43 maximum_seating 44 mileage 
# 49 salvage 51 seller_rating 56 transmission
# 61 wheel_system 63 wheelbase 64 width 65 year

next(sys.stdin) #去掉表头
for line in reader:
    try:
        #print(line)    
        assert len(line) == 66
        #df = df.append([line],ignore_index=True)

        price = line[48]

        #5 body_type
        ##将除了'SUV / Crossover'，'Sedan'的其他28%类型并为其他
        body_type_SUV = str(int('SUV / Crossover' in line[5]))
        body_type_Sedan = str(int('Sedan' in line[5]))
        line.extend(body_type_SUV)
        line.extend(body_type_Sedan)

        #13 engine_cylinders
        ##将除了'I4'，'V6'的其他28%类型并为其他
        engine_cylinders_I4 = str(int('I4' in line[13]))
        engine_cylinders_V6 = str(int('V6' in line[13]))
        line.extend(engine_cylinders_I4)
        line.extend(engine_cylinders_V6)

        #18 frame_damaged
        ##将空值改为True
        if (line[18] == ''):
            line[18] = 'True'

        #23 fuel_type
        ##将‘Gasoline'之外的变量全部设为其他
        line[23] = str(int('Gasoline' in line[23]))

        #24 has_accidents
        ##将True/False/Null设为哑变量
        has_accidents_T = str(int('True' in line[24]))
        has_accidents_F = str(int('False' in line[24]))
        line.extend(has_accidents_T)
        line.extend(has_accidents_F)

        #29 isCab
        ##将True/False/Null设为哑变量
        isCab_T = str(int('True' in line[29]))
        isCab_F = str(int('False' in line[29]))
        line.extend(isCab_T)
        line.extend(isCab_F)

        #35 length
        #if (line[35] == ''):
        #    line[35] = '0'

        #49 salvage
        ##将空值转为True
        if line[49] == '':
            line[49] = 'True'

        #56 transmission
        ##将除了'A'，'CVT'的其他4%类型并为其他
        transmission_A = str(int('A' in line[56]))
        transmission_CVT = str(int('CVT' in line[56]))
        line.extend(transmission_A)
        line.extend(transmission_CVT)

        #61 wheel_system
        ##将除了'FWD'，'AWD'的其他35%类型并为其他
        wheel_system_FWD = str(int('FWD' in line[61]))
        wheel_system_AWD = str(int('AWD' in line[61]))
        line.extend(wheel_system_FWD)
        line.extend(wheel_system_AWD)

        #63 wheelbase
        #if (line[63] == ''):
        #    line[63] = '0'

        #64 width
        #if (line[64] == ''):
        #    line[64] = '0'

        #47 power 177   hp @ 5,750 RPM   
        hp = re.split(r' hp @ ', re.sub('[",]', '', line[47]))
        #55 torque 200   lb-ft @ 1,750 RPM  +2
        torque = re.split(r' lb-ft @ ', re.sub('[",]', '', line[55]))
        line.extend(hp)
        line.extend(torque)


        for i in range(len(line)):
            line[i] = re.sub('in|seats|gal|RPM','',line[i]) #删去单位
        
        for i in range(len(line)):
            line[i] = re.sub('--','',line[i]) #将‘--’删去
    
        for i in range(len(line)):
            line[i] = re.sub('True|TRUE','1',line[i]) #将TRUE转换成1
    
        for i in range(len(line)):
            line[i] = re.sub('False|FALSE','0',line[i]) #将FALSE转换成0

        #删去不需要的变量
        for i in list(reversed(drop)):
            line.pop(i)

        #把price追加到列表最后一列
        line.append(price)
        
        #line = [float(x) for x in line]

        #将空白全补为NA
        for i in range(len(line)):
            if(line[i] == ''):
                line[i] = 'NA'

        #如果有空值，则不输出此行
        if line.count('NA') != 0:
            continue
        else:
            #print("\t".join(line))
            print(','.join(line))
    except:
        continue
    