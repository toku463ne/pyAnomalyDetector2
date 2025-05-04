"""
class to get data from zabbix postgreSQL database
"""
from data_getter.data_getter import DataGetter
from typing import Dict, List
import pandas as pd # type: ignore

from db.postgresql import PostgreSqlDB

class ZabbixGetter(DataGetter):
    history_tables = ['history', 'history_uint']
    trends_tables = ['trends', 'trends_uint']
    fields = ['itemid', 'clock', 'value']
    fields_full = ['itemid', 'clock', 'value_min', 'value_avg', 'value_max']
    db: PostgreSqlDB = None

    def init_data_source(self, data_source: Dict):
        self.db = PostgreSqlDB(data_source)
        self.api_url = data_source['api_url']

    def check_conn(self) -> bool:
        cur = self.db.exec_sql("SELECT version();")
        cnt = 0
        for row in cur:
            cnt += 1
        
        return cnt > 0

    def get_history_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        if len(itemIds) > 0:
            where_itemIds = " AND itemid = ANY(ARRAY[" + ",".join([str(itemid) for itemid in itemIds]) + "])"
        else:
            where_itemIds = ""
        
        # join history and history_uint tables
        sql = f"""
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            SELECT itemid, clock, value
            FROM history
            WHERE clock BETWEEN {startep} AND {endep}
            {where_itemIds}
            UNION ALL
            SELECT itemid, clock, value
            FROM history_uint
            WHERE clock BETWEEN {startep} AND {endep}
            {where_itemIds}
        """

        df = self.db.read_sql(sql)
        if len(df) == 0:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        # sort by itemid and clock
        df = df.sort_values(['itemid', 'clock'])
        return df
    

    def get_trends_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        if len(itemIds) > 0:
            where_itemIds = " AND itemid = ANY(ARRAY[" + ",".join([str(itemid) for itemid in itemIds]) + "])"
        else:
            where_itemIds = ""
        
        # join trends and trends_uint tables
        sql = f"""
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            SELECT itemid, clock, value_avg as value
            FROM trends
            WHERE clock BETWEEN {startep} AND {endep}
            {where_itemIds}
            UNION
            SELECT itemid, clock, value_avg as value
            FROM trends_uint
            WHERE clock BETWEEN {startep} AND {endep}
            {where_itemIds}
        """

        df = self.db.read_sql(sql)
        if len(df) == 0:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        # sort by itemid and clock
        df = df.sort_values(['itemid', 'clock'])
        return df
    
    def get_trends_full_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        if len(itemIds) > 0:
            where_itemIds = " AND itemid IN (" + ",".join([str(itemid) for itemid in itemIds]) + ")"
        else:
            where_itemIds = ""
        
        sql = f"""
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            SELECT itemid, clock, value_min, value_avg, value_max
            FROM trends
            WHERE clock >= {startep} AND clock <= {endep}
            {where_itemIds}
            UNION ALL
            SELECT itemid, clock, value_min, value_avg, value_max
            FROM trends_uint
            WHERE clock >= {startep} AND clock <= {endep}
            {where_itemIds}
        """

        df = self.db.read_sql(sql)
        if len(df) == 0:
            return pd.DataFrame(columns=['itemid', 'clock', 'value_min', 'value_avg', 'value_max'], dtype=object)
        df.columns = self.fields_full
        # sort by itemid and clock
        df = df.sort_values(['itemid', 'clock'])
        return df


    def get_itemIds(self, item_names: List[str] = [], 
                    host_names: List[str] = [], 
                    group_names: List[str] = [],
                    itemIds: List[int] = [],
                    max_itemIds = 0) -> List[int]:
        where_conds = []
        # if names includes '*', convert them to '%' and use LIKE operator
        # else use '=' operator
        names_list = [("items", item_names), ("hosts", host_names), ("hstgrp", group_names)]
        for (table_name, names) in names_list:
            if len(names) > 0:
                name_conds = []
                for name in names:
                    if '*' in name or '%' in name:
                        name_conds.append(f"{table_name}.name LIKE '{name.replace('*', '%')}'")
                    else:
                        name_conds.append(f"{table_name}.name = '{name}' OR {table_name}.name LIKE '{name}/%'")
                where_conds.append("(" + " OR ".join(name_conds) + ")")

        if len(itemIds) > 0:
            where_conds.append(" itemId IN (%s)" % ",".join(map(str, itemIds)))

        if where_conds:
            where_itemIds = "WHERE " + " AND ".join(where_conds)
        else:
            where_itemIds = ""

        limitcond = ""
        if max_itemIds > 0:
            limitcond = f" limit {max_itemIds}"
        
        sql = f"""
            SELECT items.itemid
            FROM hosts 
            inner join items on hosts.hostid = items.hostid
            inner join hosts_groups on hosts_groups.hostid = hosts.hostid
            inner join hstgrp on hstgrp.groupid = hosts_groups.groupid 
            {where_itemIds}
            {limitcond}
        """

        cur = self.db.exec_sql(sql)
        rows = cur.fetchall()
        cur.close()
        if len(rows) == 0:
            return []
        return [row[0] for row in rows]
    


    def get_item_host_dict(self, itemIds: List[int] = []) -> Dict[int, int]:
        if len(itemIds) > 0:
            where_itemIds = "WHERE itemid IN (" + ",".join([str(itemid) for itemid in itemIds]) + ")"
        else:
            where_itemIds = ""
        
        sql = f"""
            SELECT itemid, hostid
            FROM items
            {where_itemIds}
        """

        cur = self.db.exec_sql(sql)
        rows = cur.fetchall()
        cur.close()
        if len(rows) == 0:
            return {}
        return {row[0]: row[1] for row in rows}
    

    def classify_by_groups(self, itemIds: List[int], group_names: List[str]) -> Dict[str, List[int]]:
        if len(group_names) == 0:
            return {"all": itemIds}
        
        if len(itemIds) == 0:
            return {"all": []}
        
        cond_itemIds = "AND itemid IN (" + ",".join([str(itemid) for itemid in itemIds]) + ")"

        
        # get groupid from given group_names considering sub groups.
        # ex) app/sim/rp is sub group of app/sim
        groups = {}
        for group_name in group_names:
            sql = f"""
                SELECT distinct items.itemid
                FROM items 
                inner join hosts on hosts.hostid = items.hostid
                inner join hosts_groups on hosts_groups.hostid = hosts.hostid
                inner join hstgrp on hstgrp.groupid = hosts_groups.groupid 
                WHERE (hstgrp.name = '{group_name}' OR hstgrp.name LIKE '{group_name}/%') 
                {cond_itemIds}
            """

            cur = self.db.exec_sql(sql)
            rows = cur.fetchall()
            cur.close()
            if len(rows) == 0:
                continue

            group_info = [row[0] for row in rows]
            if len(group_info) > 0: 
                groups[group_name] = group_info

        return groups
            

    def get_item_relations(self, itemIds: List[int], group_names: List[str]) -> pd.DataFrame:
        df = pd.DataFrame()
        if len(itemIds) > 0:
            where_itemIds = "AND items.itemid IN (" + ",".join([str(itemid) for itemid in itemIds]) + ")"
        for name in group_names:
            where_cond = ""
            if '*' in name or '%' in name:
                where_cond = f"hstgrp.name LIKE '{name.replace('*', '%')}'"
            else:
                where_cond = f"hstgrp.name = '{name}' or hstgrp.name LIKE '{name}/%'"
            sql = f"""select '{name}' as group_name, hosts.hostid, items.itemid
                from hosts 
                inner join items on hosts.hostid = items.hostid
                inner join hosts_groups on hosts_groups.hostid = hosts.hostid
                inner join hstgrp on hstgrp.groupid = hosts_groups.groupid 
                where {where_cond}
                {where_itemIds}
            """
            df = pd.concat([df, self.db.read_sql(sql)], ignore_index=True)
        numeric_cols = df.select_dtypes(include=['float']).columns
        df[numeric_cols] = df[numeric_cols].astype(int)
        df.columns = ['group_name', 'hostid', 'itemid']
        return df



    def get_item_details(self, itemIds: List[int]) -> Dict:
        """
        get dict in the following format
        {
            <itemid>: {
                'hostid': <hostId>,
                'host_name': <hostName>,
                'item_name': <itemName>}
        }
        """
        if len(itemIds) == 0:
            return {}
        
        where_itemIds = "WHERE itemid IN (" + ",".join([str(itemid) for itemid in itemIds]) + ")"
        sql = f"""
            SELECT items.itemid, hosts.hostid, hosts.name as host_name, items.name as item_name
            FROM items 
            inner join hosts on hosts.hostid = items.hostid
            {where_itemIds}
        """

        cur = self.db.exec_sql(sql)
        rows = cur.fetchall()
        cur.close()
        if len(rows) == 0:
            return {}
        return {row[0]: {"hostid": row[1], "host_name": row[2], "item_name": row[3]} for row in rows}
    
    # check if the itemid meets the condition
    def check_itemId_cond(self, itemIds: List[int], item_cond: str) -> bool:
        if item_cond == "":
            return itemIds
        sql = f"""
            SELECT itemid
            FROM items
            WHERE itemid IN ({",".join(map(str, itemIds))}) AND {item_cond}
        """
        cur = self.db.exec_sql(sql)
        rows = cur.fetchall()
        cur.close()
        
        return [row[0] for row in rows]
    

    def get_items_details(self, itemIds: List[int]) -> pd.DataFrame:
        sql = f"""
            select hstgrp.name as group_name, hosts.hostid as hostid, hosts.host as host_name, items.itemid as itemid, items.key_ item_name
                from hosts 
                inner join items on hosts.hostid = items.hostid
                inner join hosts_groups on hosts_groups.hostid = hosts.hostid
                inner join hstgrp on hstgrp.groupid = hosts_groups.groupid 
                where itemid IN ({",".join(map(str, itemIds))})
        """

        df = self.db.read_sql(sql)
        df.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']

        return df
    

    def get_group_map(self, itemIds: List[int], group_names: List[str]) -> Dict[int, str]:
        if len(itemIds) == 0:
            return {}
        
        if len(group_names) == 0:
            return {}
        
        group_map = {}
        for group_name in group_names:
            sql = f"""
                SELECT items.itemid
                FROM hosts 
                inner join items on hosts.hostid = items.hostid
                inner join hosts_groups on hosts_groups.hostid = hosts.hostid
                inner join hstgrp on hstgrp.groupid = hosts_groups.groupid 
                WHERE (hstgrp.name = '{group_name}' OR hstgrp.name LIKE '{group_name}/%') 
                AND items.itemid IN ({",".join(map(str, itemIds))})
            """

            cur = self.db.exec_sql(sql)
            rows = cur.fetchall()
            cur.close()
            if len(rows) == 0:
                continue

            for row in rows:
                group_map[row[0]] = group_name

        return group_map
    
    def get_item_html_title(self, itemId: int, chart_type="") -> str:
        # link to zabbix chart 
        # http://{{ api_url }}/history.php?itemids%5B0%5D={{ itemid }}&period=now-30d&action=showgraph

        detail = self.get_item_detail(itemId)
        href = f"{self.api_url}/history.php?itemids%5B0%5D={itemId}&period=now-730h"
        if chart_type == "topitems":
            href += f"&chart_type={chart_type}"

        return f"""<a href="{href}" target="_blank">
        {detail["host_name"][:50]}<br>
        {detail["item_name"][:50]}<br>
        {itemId}</a>"""