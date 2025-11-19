#!/usr/bin/env python3
import mysql.connector
from mysql.connector import Error

def main():
    try:
        # 1. 连接
        print("connecting...")
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='test',
            password='123456',
            # database='employees',
            # use_unicode=True,
            # charset='utf8mb4'
        )
        print("connected.")
        if conn.is_connected():
            cursor = conn.cursor(prepared=True)  # 启用prepared statements

            # 准备SQL语句
            query = "SELECT * FROM users WHERE age > ? AND city = ?"
            # 执行参数化查询
            params = (25, 'Beijing')
            cursor.execute(query, params)

            # 获取结果
            results = cursor.fetchall()

            for row in results:
                print(row)

        # 4. 关闭
        cursor.close()
        conn.close()

    except Error as e:
        print('MySQL 错误:', e)

if __name__ == '__main__':
    main()
