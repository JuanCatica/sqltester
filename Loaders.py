import time
import math
import _thread
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from tqdm import tqdm
from IPython.display import display, clear_output
import ipywidgets as widgets

class SQLLoader:
    """
    
    """
    AVAILABLE = "AVAILABLE"
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    DESTROYED = "DESTOYED"
    
    def __init__(self, file, dbengine, dbhost, dbuser, dbpass, dbport, dbname, dbtable, drop=False, dtype={}, dtype_db={}, date_fields=[]):
        """
        
        """
        self.file = file
        self.engine = create_engine(f'{dbengine}://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}')
        self.dbtable = dbtable
        self.dtype_db = dtype_db
        
        self.iteration = 0
        self.registers_inserted = 0
        self.registers_updated = 0
        self.registers_deleted = 0
        
        self.__status = {
            "state": self.AVAILABLE,
            "iteration":self.iteration,
            "inserted":self.registers_inserted,
            "updated":self.registers_updated,
            "deleted":self.registers_deleted,
            "indb":0
        }
        
        self.df = pd.read_csv(f"{self.file}",dtype=dtype, parse_dates=date_fields)
        self.df["_insert"] = 0
        self.df["_insert_time"] = 0
        self.df["_update"] = 0
        self.df["_update_time"] = 0
        self.df["_delete"] = 0
        self.df["_delete_time"] = 0
        
        if drop:
            self.drop_table() 
    
        # -----------
        # UI ELEMENTS
        # 
        self.out = widgets.Output()
        start_button = widgets.Button(description='Start')
        stop_button = widgets.Button(description='Stop')
        destroy_button = widgets.Button(description='Destroy Process')
        
        def start_button_clicked(_):
            self.__status["state"] = self.RUNNING
            
        def stop_button_clicked(_):
            self.__status["state"] = self.STOPPED
            
        def destroy_button_clicked(_):
            self.__status["state"] = self.DESTROYED
            
        start_button.on_click(start_button_clicked)
        stop_button.on_click(stop_button_clicked)
        destroy_button.on_click(destroy_button_clicked)
        
        self.buttons = widgets.HBox([start_button, stop_button, destroy_button])    
        self.box = widgets.VBox([self.buttons, self.out])
        
    def __get_message(self, in_db="", i="", u="", d="", i_s="", u_s="", d_s="", loop_perc="", db_perc=""):
        """
        
        """
        state = self.__status["state"]
        play = "" if state != self.RUNNING else "▶" if self.iteration%2==0 else "·" 
        
        state_db = f"{play}[{state}] DB:{in_db}"
        uid = f"I:{i}, U:{u}, D:{d}"
        uid_s = f"I/s:{i_s:.0f}, U/s:{u_s:.0f}, D/s:{d_s:.0f}"
        percent = f"Loop:{loop_perc:.2f}%, DF:{db_perc:.2f}%"
        
        message = f"{state_db} | {uid} | {uid_s} | {percent}"
        return message
    
    def __printUI(self, message):
        """
        
        """
        with self.out:
            clear_output(wait=True)
            display(message)
    
    def insert(self, registers=10, delay=0):
        """
        
        """
        temp_df = self.df.iloc[self.registers_inserted : self.registers_inserted + registers].copy()
        temp_df["_insert"] = 1        
        temp_df["_insert_time"] = time.time()
        temp_df.drop(["_delete","_delete_time"],axis=1).to_sql(
            name=self.dbtable,
            con=self.engine,
            if_exists="append",
            dtype=self.dtype_db
        )
        self.df.iloc[self.registers_inserted : self.registers_inserted + registers] = temp_df
        self.registers_inserted += registers
        time.sleep(delay)
    
    def update(self, registers=5, delay=0):
        """
        
        """
        if registers > 0:
            temp_df = self.df.iloc[:self.registers_inserted][self.df._delete==0].sample(n=registers).copy()
            temp_df["_update"] = temp_df["_update"] + 1
            temp_df["_update_time"] = time.time()

            indexes_str = map(lambda x: str(x), temp_df.index.to_list())
            indexes = ", ".join(indexes_str)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    UPDATE {self.dbtable} 
                    SET "_update" = "_update" + 1, "_update_time" = {int(time.time())} 
                    WHERE "index" IN ({indexes})
                """))
            
            self.df[self.df.index.isin(temp_df.index)] = temp_df.copy()
            self.registers_updated += len(temp_df)
            time.sleep(delay)
    
    def delete(self, registers=1, delay=0):
        """
        
        """
        if registers > 0:
            temp_df = self.df.iloc[:self.registers_inserted][self.df._delete==0].sample(n=registers).copy()
            temp_df["_delete"] = temp_df["_delete"] + 1
            temp_df["_delete_time"] = time.time()

            indexes_str = map(lambda x: str(x), temp_df.index.to_list())
            indexes = ", ".join(indexes_str)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    DELETE FROM {self.dbtable}
                    WHERE "index" IN ({indexes})
                """))
                
            self.df[self.df.index.isin(temp_df.index)] = temp_df.copy()
            self.registers_deleted += len(temp_df)
            time.sleep(delay)

    def iud(self, inserts=10, updates=5, deletes=0, delay=0, max_registers=None, uix=False):
        """
        
        """     
        # VALIDATIONS
        if self.__status["state"] == self.RUNNING or self.__status["state"] == self.STOPPED:
            print("There is already a 'iudx' load in progress, please destroy the process.")
            return
        assert inserts > 0, "'inserts' must be grather than 0."
        assert inserts > deletes, "'inserts' must be grather than 'deletes'."
        
        # INITIATING STATE AND VARS
        self.__status["state"] = self.RUNNING  
        message = ""
        in_db = None
        inserted = None
        updated = None
        deleted = None
        i_s = None
        u_s = None
        d_s = None
        loop_perc = None
        df_perc = None
        
        # CALCULATING ITERATIONS
        not_inserted = len(self.df[self.df["_insert"]==0])
        max_registers = max_registers if max_registers else not_inserted
        num_iters = math.ceil(min(max_registers/float(inserts), not_inserted/float(inserts)))
        iterations = range(num_iters) if uix else tqdm(range(num_iters))
        
        # MAIN LOOP
        for i in iterations:            
            start = time.time()
            
            # INERTS | UPDATE | DELETES
            self.insert(inserts)
            self.update(updates)
            self.delete(deletes)
            
            # QUERY STATUS AND DELAY
            st = self.status()
            time.sleep(delay)
            delta = float(time.time() - start)
            
            # PROCESS STATS
            in_db = st["indb"]
            inserted = st["inserted"]
            updated = st["updated"]
            deleted = st["deleted"] 
            i_s = inserts/delta
            u_s = updates/delta
            d_s = deletes/delta
            loop_perc = 100.0*i/float(num_iters)
            df_perc = 100.0*self.registers_inserted/len(self.df)
            self.iteration +=1
            
            # MESSAGE
            message = self.__get_message(in_db, inserted, updated, deleted, i_s, u_s, d_s, loop_perc, df_perc)
            
            # PRINT MESSAGE: TQDM | UI WIDGETS
            if uix==False:
                iterations.set_description(message)
            else:
                self.__printUI(message)
                
            # STOP LOOP
            while self.__status["state"] == self.STOPPED:
                time.sleep(0.01)
               
            # DESTROY
            if self.__status["state"] == self.DESTROYED:
                self.__printUI(message)
                break
                
        # LAST MESSAGE
        self.__status["state"] = self.AVAILABLE
        message = self.__get_message(in_db, inserted, updated, deleted, i_s, u_s, d_s, loop_perc, df_perc)
        clear_output(wait=True)
        print(message)
    
    def iudx(self, inserts=10, updates=5, deletes=0, delay=0):
        """
        
        """
        if self.__status["state"] == self.RUNNING or self.__status["state"] == self.STOPPED:
            print("There is already a 'iudx' load in progress")
        else:
            _thread.start_new_thread(self.iud, (inserts, updates , deletes, delay, None, True))
        return self.box

    def status(self):
        """
        
        """
        try:
            registers_in_db = list(self.engine.execute(f'SELECT COUNT(*) FROM {self.dbtable}'))[0][0]
        except:
            registers_in_db = None     
        self.__status["iteration"] = self.iteration
        self.__status["inserted"] = self.registers_inserted
        self.__status["updated"] = self.registers_updated
        self.__status["deleted"] = self.registers_deleted
        self.__status["indb"] = registers_in_db
        return self.__status
        
    def drop_table(self):
        """
        
        """
        self.iteration = 0
        self.registers_inserted = 0
        self.registers_updated = 0
        self.registers_deleted = 0
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.dbtable}
                """))
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    DROP TABLE {self.dbtable}
                """))
        except:
            print("ERROR!!!")