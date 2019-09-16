using System;
using System.Data;
using System.Reflection;
using System.Data.Entity;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Common.Provider;
using VidaGISCloud.Models;
using System.Data.SqlClient;

namespace VidaGISCloud.Services
{
    public class ObjectTennantLocalServiceUtil<T> where T : class //: ObjectLocalServiceImpl<T> where T : class
    {
        private DataTennantConnection _dbTennant;
        private DataTennantSlaveConnection _dbTennant_Slave;
        private DbContext _db;
        protected DbSet<T> _objInfo;
        protected DbSet<T> _objSlaveInfo;
        protected string _tablename;
        protected postgreSQL _provider;
        protected string schema = "public";
        private bool _slave = true; // true: read slave, false: read master

        /// <summary>
        /// LOG FILE 
        /// </summary>
        static Type type = typeof(T);
        protected Common.Log.LogWrapper _log = Common.Log.LogFactoryUtil.getLog(type.ToString()) as Common.Log.LogWrapper;

        /*protected static ObjectTennantLocalServiceUtil<T> _Instance;
        public static ObjectTennantLocalServiceUtil<T> Instance
        {
            get
            {
                if (_Instance == null)
                    _Instance = new ObjectTennantLocalServiceUtil<T>();

                return _Instance;
            }
        }*/

        private DbParameter createParameter()
        {
            return this._provider.createParameter();
        }

        protected DbParameter[] createParameters(Dictionary<string, object> dic)
        {
            DbParameter[] parameters = new DbParameter[dic.Count];
            try
            {
                int index = 0;
                foreach (KeyValuePair<string, object> kvp in dic)
                {
                    string key = kvp.Key;
                    object value = kvp.Value;

                    DbParameter param = this.createParameter();
                    param.ParameterName = key;
                    param.Value = value;
                    parameters[index++] = param;
                }
            }
            catch (Exception ex)
            {
                parameters = new DbParameter[0];
                _log.error(ex.ToString());
            }
            return parameters;
        }


        public string getClassName()
        {
            return (typeof(T).FullName);
        }

        public ObjectTennantLocalServiceUtil()
        {
            this._dbTennant = new DataTennantConnection();
            this._dbTennant_Slave = new DataTennantSlaveConnection();
            if (this._dbTennant != null) this._objInfo = this._dbTennant.Set<T>();
            if (this._dbTennant_Slave != null) this._objSlaveInfo = this._dbTennant_Slave.Set<T>();
            if (_slave) this._db = this._dbTennant;
            else this._db = this._dbTennant_Slave;

            if (this._db != null) this._provider = new postgreSQL(this._db);

            // add className contain Model into database to pemission - only run one when start system
            //this.updateClassName();
        }

        private void updateClassName()
        {
            //Type[] typelist = GetTypesInNamespace(Assembly.GetExecutingAssembly(), "VidaGISCloud");
            List<Type> listType  = Assembly.GetExecutingAssembly().GetTypes().ToList();
            for (int i = 0; i < listType.Count; i++)
            {
                Type type = listType[i]; 
                //Console.WriteLine(typelist[i].Name);
                string className = type.FullName;
                if (!className.ToLower().Contains("model")) continue;
                string sql = "select * from vidagis_classname where vidagis_name = '" + className + "'";
                DataTable list = this.excuteDataTableQuery(sql, className);
                try
                {
                    if (list.Rows.Count == 0)
                    {
                        this._log.info("Add classname into database: " + className);
                        string classnameId = Common.Utility.getMilisecond1970().ToString();
                        string date = Common.Utility.getDate("yyyy-MM-dd", DateTime.Now);
                        sql = "insert into vidagis_classname(vidagis_classnameid,vidagis_name,vidagis_title,vidagis_dateupdate) values('" + classnameId + "','" +
                            className + "','" + className + "','" + date + "')";
                        this.excuteNonQuery(sql, null);
                    }
                }
                catch (Exception ex)
                {
                    this._log.error("Error add classname " + className + ": " + ex.Message);
                }
                finally
                {

                }
            }
        }

        private Type[] GetTypesInNamespace(Assembly assembly, string nameSpace)
        {
            return assembly.GetTypes().Where(t => String.Equals(t.Namespace, nameSpace, StringComparison.Ordinal)).ToArray();
        }

        /// <summary>
        /// get connnection database master to write data
        /// </summary>
        /// <returns></returns>
        protected DataTennantConnection getConnectionMaster()
        {
            return this._dbTennant;
        }

        /// <summary>
        /// get connnection database master to write data
        /// </summary>
        /// <returns></returns>
        protected DataTennantSlaveConnection getConnectionSlave()
        {
            return this._dbTennant_Slave;
        }

        #region for LINQ

        public List<T> getAll()
        {
            return this._objSlaveInfo.ToList();
        }
        public T getByID(long oid)
        {
            T t = null;
            t = this._objSlaveInfo.Find(oid);
            return t;
        }
        public T add(T obj)
        {
            
            T t = this._objInfo.Add(obj);
            this.SaveData();
            this._db.Entry(obj).State = System.Data.Entity.EntityState.Detached;
            
            return t;
        }

        public T update(T obj)
        {
            
            this._db.Entry(obj).State = System.Data.Entity.EntityState.Modified;

            this.SaveData();
            this._db.Entry(obj).State = System.Data.Entity.EntityState.Detached;
            return obj;
        }


        public bool update(List<T> lObj)
        {
            try
            {
                foreach (var obj in lObj)
                {
                    this._db.Entry(obj).State = System.Data.Entity.EntityState.Modified;
                }
                this.SaveData();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public T del(long oid)
        {
            T t = this.getByID(oid);
            this._db.Entry(t).State = System.Data.Entity.EntityState.Deleted;
            if (t != null) this.del(t);
            return t;
        }
        public T del(T obj)
        {
            this._db.Entry(obj).State = System.Data.Entity.EntityState.Deleted;
            T t = this._objInfo.Remove(obj);
            this.SaveData();
            return t;
        }
        public int SaveData()
        {
            return this._db.SaveChanges();
        }
        #endregion

        #region for Query String

        public DataTable gets(string fieldOrderBy)
        {
            string sql = "";
            if (String.IsNullOrEmpty(fieldOrderBy)) sql = "select * from " + this._tablename;
            else sql = "select * from " + this._tablename + " order by " + fieldOrderBy;
            DataTable dt = this.excuteDataTableQuery(sql, this._tablename);
            return dt;
        }

        public DataTable getsDataTable(int start, int end, string fieldOrderBy)
        {
            int offset = start == -1 ? 0 : start - 1;
            int limit = end - start + 1;
            if (end == -1) limit = 999999999;
            else limit = end - offset;
            string sql = "";
            if (String.IsNullOrEmpty(fieldOrderBy)) sql = "select * from " + this._tablename + " offset " + offset + " limit " + limit;
            else sql = "select * from " + this._tablename + " order by " + fieldOrderBy + " offset " + offset + " limit " + limit;
            DataTable dt = this.excuteDataTableQuery(sql, this._tablename);
            return dt;
        }

        public DataTable excuteDataTableQuery(string sql, string tablename)
        {
            DataTable dt = this._provider.getDatatable(sql, null);
            return dt;
        }

        public DataTable excuteDataTableQuery(string sql, DbParameter[] parameters = null)
        {
            DataTable dt = this._provider.getDatatable(sql, parameters);
            return dt;
        }

        public List<T> executeQuery(string sql, System.Data.SqlClient.SqlParameter[] parameters = null, int? CommandTimeout = null)
        {
            DataTable dt = this._provider.getDatatable(sql, parameters, CommandTimeout);
            List<T> lst = new List<T>();
            int i = 0;
            DataColumnCollection dclCollection = dt.Columns;
            for (i = 0; i < dt.Rows.Count; i++)
            {
                DataRow drw = dt.Rows[i];
                T t = (T)Activator.CreateInstance(typeof(T));
                //System.Reflection.PropertyInfo[] proInfos = drw.GetType().GetProperties();
                System.Reflection.PropertyInfo[] proInfosT = t.GetType().GetProperties();
                try
                {
                    foreach (DataColumn dcl in dclCollection)
                    {
                        string proName = dcl.ColumnName;
                        object proVal = drw[proName];
                        if (proVal == DBNull.Value) continue;

                        foreach (PropertyInfo proInfoT in proInfosT)
                        {
                            string proNameT = proInfoT.Name;
                            if (proName == proNameT)
                            {
                                proInfoT.SetValue(t, proVal);
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                lst.Add(t);
            }

            return lst;
        }
        
        public List<T> executeQuery<T>(string sql, System.Data.SqlClient.SqlParameter[] parameters = null)
        {
            DataTable dt = this._provider.getDatatable(sql, parameters);
            List<T> lst = new List<T>();
            int i = 0;
            DataColumnCollection dclCollection = dt.Columns;
            for (i = 0; i < dt.Rows.Count; i++)
            {
                DataRow drw = dt.Rows[i];
                T t = (T)Activator.CreateInstance(typeof(T));
                //System.Reflection.PropertyInfo[] proInfos = drw.GetType().GetProperties();
                System.Reflection.PropertyInfo[] proInfosT = t.GetType().GetProperties();
                try
                {
                    foreach (DataColumn dcl in dclCollection)
                    {
                        string proName = dcl.ColumnName;
                        object proVal = drw[proName];
                        if (proVal == DBNull.Value) continue;

                        foreach (PropertyInfo proInfoT in proInfosT)
                        {
                            string proNameT = proInfoT.Name;
                            if (proName == proNameT)
                            {
                                proInfoT.SetValue(t, proVal);
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                lst.Add(t);
            }

            return lst;
        }
        
        public List<T> excuteListQuery<T>(string sql, DbParameter[] parameters = null)
        {
            DataTable dt = this._provider.getDatatable(sql, parameters);
            List<T> lst = new List<T>();
            int i = 0;
            DataColumnCollection dclCollection = dt.Columns;
            for (i = 0; i < dt.Rows.Count; i++)
            {
                DataRow drw = dt.Rows[i];
                T t = (T)Activator.CreateInstance(typeof(T));
                //System.Reflection.PropertyInfo[] proInfos = drw.GetType().GetProperties();
                System.Reflection.PropertyInfo[] proInfosT = t.GetType().GetProperties();
                try
                {
                    foreach (DataColumn dcl in dclCollection)
                    {
                        string proName = dcl.ColumnName;
                        object proVal = drw[proName];
                        if (proVal == DBNull.Value) continue;

                        foreach (PropertyInfo proInfoT in proInfosT)
                        {
                            string proNameT = proInfoT.Name;
                            if (proName == proNameT)
                            {
                                proInfoT.SetValue(t, proVal);
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                lst.Add(t);
            }

            return lst;
        }

        public List<T> excuteListQuery(string sql, int start, int end, string tablename)
        {

            throw new NotImplementedException();
        }

        public int excuteNonQuery(string sqlNonQuery, System.Data.SqlClient.SqlParameter[] parameters)
        {
            return this._provider.excuteNonQuery(sqlNonQuery, parameters);
        }

        public object executeScalar(string sqlQuery, DbParameter[] parameters = null)
        {
            return this._provider.executeScalar(sqlQuery, parameters);
        }


        public T getObject(string sql, System.Data.SqlClient.SqlParameter[] parameters = null, int? CommandTimeout = null)
        {
            List<T> lst = this.executeQuery(sql, parameters, CommandTimeout);
            if (lst.Count > 0) return lst[0];
            return null;
        }
        public T getObject<T>(string sql, System.Data.SqlClient.SqlParameter[] parameters = null)
        {
            List<T> lst = this.executeQuery<T>(sql, parameters);
            if (lst.Count > 0) return lst[0];
            return default(T);
        }

        public List<T> ConvertDatatableToList<T>(DataTable dt)
        {
            //var columnNames = dt.Columns.Cast<DataColumn>()
            //        .Select(c => c.ColumnName)
            //        .ToList();
            //var properties = typeof(T).GetProperties();
            //return dt.AsEnumerable().Select(row =>
            //{
            //    var objT = Activator.CreateInstance<T>();
            //    foreach (var pro in properties)
            //    {
            //        if (columnNames.Contains(pro.Name))
            //        {
            //            PropertyInfo pI = objT.GetType().GetProperty(pro.Name);
            //            pro.SetValue(objT, row[pro.Name] == DBNull.Value ? null : Convert.ChangeType(row[pro.Name], pI.PropertyType),null);
            //        }
            //    }
            //    return objT;
            //}).ToList();


            List<T> lst = new List<T>();
            int i = 0;
            DataColumnCollection dclCollection = dt.Columns;
            for (i = 0; i < dt.Rows.Count; i++)
            {
                DataRow drw = dt.Rows[i];
                T t = (T)Activator.CreateInstance(typeof(T));
                //System.Reflection.PropertyInfo[] proInfos = drw.GetType().GetProperties();
                System.Reflection.PropertyInfo[] proInfosT = t.GetType().GetProperties();
                try
                {
                    foreach (DataColumn dcl in dclCollection)
                    {
                        string proName = dcl.ColumnName;
                        object proVal = drw[proName];
                        if (proVal == DBNull.Value) continue;

                        foreach (PropertyInfo proInfoT in proInfosT)
                        {
                            string proNameT = proInfoT.Name;
                            if (proName == proNameT)
                            {
                                proInfoT.SetValue(t, proVal);// proInfoT.SetValue(t, Convert.ChangeType(proVal, proInfoT.PropertyType));
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                lst.Add(t);
            }

            return lst;

        }
        #endregion

        #region utility
        public string getTableName(string tableid)
        {
            return "vidagis_a_" + tableid.Replace("-", "");
        }

        public void moveItem(List<T> list, int oldIndex, int newIndex)
        {
            // exit if possitions are equal or outside array
            if ((oldIndex == newIndex) || (0 > oldIndex) || (oldIndex >= list.Count) || (0 > newIndex) ||
                (newIndex >= list.Count)) return;
            // local variables
            var i = 0;
            T tmp = list[oldIndex];
            // move element down and shift other elements up
            if (oldIndex < newIndex)
            {
                for (i = oldIndex; i < newIndex; i++)
                {
                    list[i] = list[i + 1];
                }
            }
            // move element up and shift other elements down
            else
            {
                for (i = oldIndex; i > newIndex; i--)
                {
                    list[i] = list[i - 1];
                }
            }
            // put element from position 1 to destination
            list[newIndex] = tmp;
        }

        public void moveItem(List<T> list, T obj, int newIndex)
        {
            int oldIndex = -1;
            // local variables
            var i = 0;

            // find oldIndex in list
            object oid = -1;
            PropertyInfo pInfo = obj.GetType().GetProperty("oid");
            if (pInfo == null) return;
            oid = pInfo.GetValue(obj);

            for (i = 0; i < list.Count; i++)
            {
                PropertyInfo _pInfo = list[i].GetType().GetProperty("oid");
                if (_pInfo == null) continue;
                if (_pInfo.GetValue(list[i]).Equals(oid))
                {
                    oldIndex = i;
                    break;
                }
            }

            this.moveItem(list, oldIndex, newIndex);
        }
        #endregion

        #region cache
        public List<T> UpdateCache(string cacheName, int exprise, string spName, SqlParameter[] parameters)
        {
            List<T> lst = null;
            try
            {
                if (Common.CacheManager<T>.Instance.hasKey(cacheName)) Common.CacheManager<T>.Instance.ClearCache(cacheName);

                if (Common.CacheManager<T>.Instance.hasKey(cacheName)) lst = Common.CacheManager<T>.Instance.GetCache(cacheName).ToList();
                else
                {
                    lst = this.executeQuery(spName, parameters);
                    Common.CacheManager<T>.Instance.AddCache(cacheName, lst, (int)exprise);
                }

                return lst;

            }
            catch (Exception ex)
            {
                this._log.error(ex.ToString());
                return null;
            }
        }

        public List<T> UpdateCache(string cacheName, int exprise, string spName, List<SqlParameter> parameters)
        {
            List<T> lst = null;
            try
            {
                if (Common.CacheManager<T>.Instance.hasKey(cacheName)) Common.CacheManager<T>.Instance.ClearCache(cacheName);

                if (Common.CacheManager<T>.Instance.hasKey(cacheName)) lst = Common.CacheManager<T>.Instance.GetCache(cacheName).ToList();
                else
                {
                    lst = ConvertDatatableToList<T>(excuteDataTableQuery(spName, parameters.ToArray()));
                    Common.CacheManager<T>.Instance.AddCache(cacheName, lst, (int)exprise);
                }

                return lst;

            }
            catch (Exception ex)
            {
                this._log.error(ex.ToString());
                return null;
            }
        }

        /// <summary>
        /// clear cache
        /// </summary>
        /// <param name="cacheName"></param>
        /// <returns></returns>
        public bool ClearCache(string cacheName)
        {
            try
            {
                if (Common.CacheManager<T>.Instance.hasKey(cacheName)) Common.CacheManager<T>.Instance.ClearCache(cacheName);
                return true;
            }
            catch (Exception ex)
            {
                this._log.error(ex.ToString());
                return false;
            }
        }
        public bool ClearAllCache()
        {
            try
            {
                Common.CacheManager<T>.Instance.ClearAllCache();
                return true;
            }
            catch (Exception ex)
            {
                this._log.error(ex.ToString());
                return false;
            }
        }
        #endregion
    }
}