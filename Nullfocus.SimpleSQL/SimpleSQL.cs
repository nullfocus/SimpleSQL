using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Common.Logging;

namespace Nullfocus.SimpleSQL
{
    public static class SimpleSql
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private static readonly Type StringType = "".GetType();
        private static Regex paramRegex = new Regex("@(?<paramname>\\w+)");
        private static Dictionary<string, object> CachedObjectBuilders = new Dictionary<string, object>();
        private static Dictionary<string, object> CachedParameterBuilders = new Dictionary<string, object>();

        //------- public static methods ---------------------------------------

        public static void ExecuteNonQuery(IDbConnection connection, string sqlStr)
        {
            Log.Debug("ExecuteNonQuery with query:\r\n" + sqlStr);

            Log.Debug("Creating command on connection and setting command text...");

            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = sqlStr;
                command.Connection = connection;
                command.ExecuteNonQuery();
            }
        }

        public static void ExecuteObjectNonQuery(IDbConnection connection, string sqlStr, object obj)
        {
            Log.Debug("ExecuteObjectNonQuery using object:\r\n" + obj.ToString() + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromObject(connection, sqlStr, obj))
            {
                command.Connection = connection;
                command.ExecuteNonQuery();
            }
        }

        public static void ExecuteParamsNonQuery(IDbConnection connection, string sqlStr, params object[] parameters)
        {
            Log.Debug("ExecuteParamsNonQuery using params:\r\n" + string.Join(",", parameters) + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromParams(connection, sqlStr, parameters))
            {
                command.Connection = connection;
                command.ExecuteNonQuery();
            }
        }

        public static T ExecuteForScalar<T>(IDbConnection connection, string sqlStr)
        {
            Log.Debug("ExecuteForScalar with query:\r\n" + sqlStr);

            Log.Debug("Creating command on connection and setting command text...");

            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = sqlStr;
                command.Connection = connection;

                return ExecuteForScalar<T>(command);
            }
        }

        public static T ExecuteObjectForScalar<T>(IDbConnection connection, string sqlStr, object obj)
        {
            Log.Debug("ExecuteObjectForScalar using object:\r\n" + obj.ToString() + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromObject(connection, sqlStr, obj))
            {
                command.Connection = connection;

                return ExecuteForScalar<T>(command);
            }
        }

        public static T ExecuteParamsForScalar<T>(IDbConnection connection, string sqlStr, params object[] parameters)
        {
            Log.Debug("ExecuteParamsForScalar using params:\r\n" + string.Join(",", parameters) + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromParams(connection, sqlStr, parameters))
            {
                command.Connection = connection;

                return ExecuteForScalar<T>(command);
            }
        }

        public static List<T> ExecuteForList<T>(IDbConnection connection, string sqlStr)
        {
            Log.Debug("ExecuteForList with query:\r\n" + sqlStr);

            Log.Debug("Creating command on connection and setting command text...");

            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = sqlStr;
                command.Connection = connection;

                return ExecuteForList<T>(command);
            }
        }

        public static List<T> ExecuteObjectForList<T>(IDbConnection connection, string sqlStr, object obj)
        {
            Log.Debug("ExecuteObjectForList using object:\r\n" + obj.ToString() + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromObject(connection, sqlStr, obj))
            {
                command.Connection = connection;

                return ExecuteForList<T>(command);
            }
        }

        public static List<T> ExecuteParamsForList<T>(IDbConnection connection, string sqlStr, params object[] parameters)
        {
            Log.Debug("ExecuteParamsForList using params:\r\n" + string.Join(",", parameters) + "\r\nwith query:\r\n" + sqlStr);

            using (IDbCommand command = BuildCmdFromParams(connection, sqlStr, parameters))
            {
                command.Connection = connection;

                return ExecuteForList<T>(command);
            }
        }

        //------- private static helper methods ---------------------------------------

        private static Action<IDbCommand, object> GetParamterBuilder(string sqlStr, Type objType)
        {
            object paramBuilder = null;

            string key = objType.FullName + " - " + sqlStr;

            lock (CachedParameterBuilders)
            {
                if (!CachedParameterBuilders.TryGetValue(key, out paramBuilder))
                {
                    Log.Debug("First time setting query params from this object type [" + objType.Name + "], generating code...");

                    DateTime generateStart = DateTime.Now;

                    List<Expression> expressions = new List<Expression>();

                    ParameterExpression obj = Expression.Parameter(typeof(object), "newObj");
                    ParameterExpression cmd = Expression.Parameter(typeof(IDbCommand), "cmd");

                    ParameterExpression convertedObj = Expression.Parameter(objType, "val");

                    expressions.Add(Expression.Assign(convertedObj, Expression.Convert(obj, objType)));

                    MethodInfo SetParameterMethod = typeof(SimpleSql).GetMethod("SetParameter", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

                    List<string> paramNames = new List<string>();

                    Match paramMatch = paramRegex.Match(sqlStr);

                    while (paramMatch.Success)
                    {
                        string val = paramMatch.Groups["paramname"].Value.ToLower();

                        if (!paramNames.Contains(val))
                            paramNames.Add(val);

                        paramMatch = paramMatch.NextMatch();
                    }

                    Dictionary<string, PropertyInfo> properties = objType.GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);

                    foreach (string paramName in paramNames)
                    {
                        if (!properties.ContainsKey(paramName))
                            throw new ArgumentException("Object type [" + objType.FullName + "] doesn't contain property for sql parameter [" + paramName + "]!");

                        PropertyInfo property = properties[paramName];

                        Log.Debug("  Mapping property [" + objType.Name + "].[" + property.Name + "] to parameter [" + paramName + "]");

                        if (property.PropertyType.IsPrimitive)
                            expressions.Add(Expression.Call(SetParameterMethod, cmd, Expression.Constant(paramName),
                                                              Expression.Convert(
                                Expression.Property(convertedObj, property),
                                typeof(object))));
                        else
                            expressions.Add(Expression.Call(SetParameterMethod, cmd, Expression.Constant(paramName), Expression.Property(convertedObj, property)));
                    }

                    BlockExpression block = Expression.Block(
                        new[] { convertedObj },
                        expressions.ToArray()
                    );

                    paramBuilder = Expression.Lambda<Action<IDbCommand, object>>(block, cmd, obj).Compile();

                    Log.Debug("Finished generating and compiling in [" + DateTime.Now.Subtract(generateStart).TotalMilliseconds + "] ms");

                    CachedParameterBuilders.Add(key, paramBuilder);
                }
            }

            return (Action<IDbCommand, object>)paramBuilder;
        }

        private static Func<IDataReader, T> GetObjectBuilder<T>(IDbCommand command, IDataReader reader)
        {
            object objBuilder = null;

            Type type = typeof(T);

            string key = type.FullName + " - " + command.CommandText;

            lock (CachedObjectBuilders)
            {
                if (!CachedObjectBuilders.TryGetValue(key, out objBuilder))
                {
                    Log.Debug("First time building object type [" + type.Name + "] from query results, generating code...");

                    DateTime generateStart = DateTime.Now;

                    List<Expression> expressions = new List<Expression>();

                    ParameterExpression newObj = Expression.Parameter(type, "newObj");
                    ParameterExpression dataReader = Expression.Parameter(typeof(IDataReader), "dataReader");

                    expressions.Add(Expression.Assign(newObj, Expression.New(type)));

                    Dictionary<string, PropertyInfo> properties = new Dictionary<string, PropertyInfo>();

                    MethodInfo GetFieldMethod = typeof(SimpleSql).GetMethod("GetFieldFromReader", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

                    foreach (PropertyInfo prop in type.GetProperties())
                        properties.Add(prop.Name.ToLower(), prop);

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string fieldName = reader.GetName(i).ToLower();

                        if (properties.ContainsKey(fieldName))
                        {
                            PropertyInfo property = properties[fieldName];

                            Log.Debug("  Mapping column [" + fieldName + "] to [" + type.Name + "].[" + property.Name + "]");

                            BinaryExpression setField = Expression.Assign(
                                Expression.Property(newObj, property),
                                Expression.Convert(Expression.Call(GetFieldMethod, dataReader, Expression.Constant(i)), property.PropertyType));

                            expressions.Add(setField);
                        }
                    }

                    LabelTarget returnLabel = Expression.Label(type);

                    expressions.Add(Expression.Return(returnLabel, newObj, type));
                    expressions.Add(Expression.Label(returnLabel, newObj));

                    BlockExpression block = Expression.Block(
                        new[] { newObj },
                        expressions.ToArray()
                    );

                    objBuilder = Expression.Lambda<Func<IDataReader, T>>(block, dataReader).Compile();

                    Log.Debug("Finished generating and compiling in [" + DateTime.Now.Subtract(generateStart).TotalMilliseconds + "] ms");

                    CachedObjectBuilders.Add(key, objBuilder);
                }
            }

            return (Func<IDataReader, T>)objBuilder;
        }

        private static IDbCommand BuildCmdFromObject(IDbConnection connection, string sqlStr, object obj)
        {
            Log.Debug("Creating command on connection and setting command text...");

            IDbCommand command = connection.CreateCommand();
            command.CommandText = sqlStr;

            Log.Debug("Adding parameters...");

            Action<IDbCommand, object> builder = GetParamterBuilder(sqlStr, obj.GetType());

            builder(command, obj);

            return command;
        }

        private static IDbCommand BuildCmdFromParams(IDbConnection connection, string sqlStr, params object[] parameters)
        {
            Log.Debug("Creating command on connection and setting command text...");

            IDbCommand command = connection.CreateCommand();
            command.CommandText = sqlStr;

            Log.Debug("Adding parameters...");

            for (var i = 1; i <= parameters.Length; i++)
            {
                IDbDataParameter parameter = command.CreateParameter();
                parameter.ParameterName = i.ToString();
                parameter.Value = parameters[i - 1];

                Log.Debug("  [" + parameter.ParameterName + "] = [" + parameter.Value + "]");

                command.Parameters.Add(parameter);
            }

            return command;
        }

        private static T ExecuteForScalar<T>(IDbCommand command)
        {
            Type objType = typeof(T);
            bool primitive = objType.IsPrimitive || objType == StringType;

            Log.Debug("Executing query for single " + (primitive ? "primitive" : "object") + " of type [" + objType.FullName.ToString() + "]");

            T newItem = default(T);

            Func<IDataReader, T> objBuilder = null;

            Log.Debug("Executing query...");

            DateTime queryStart = DateTime.Now;

            using (IDataReader reader = command.ExecuteReader())
            {
                Log.Debug("Query results took [" + DateTime.Now.Subtract(queryStart).TotalMilliseconds + "] ms");

                if (!primitive)
                    objBuilder = GetObjectBuilder<T>(command, reader);

                DateTime readerStart = DateTime.Now;

                if (reader.Read())
                {
                    if (primitive)
                        newItem = (T)Convert.ChangeType(reader.GetValue(0), objType);
                    else
                        newItem = objBuilder(reader);
                }
            }

            Log.Debug("Completed reading from results in [" + DateTime.Now.Subtract(queryStart).TotalMilliseconds + "] ms, returning " + (primitive ? "primitive" : "object"));

            return newItem;
        }

        private static List<T> ExecuteForList<T>(IDbCommand command)
        {
            Type objType = typeof(T);
            bool primitive = objType.IsPrimitive || objType == StringType;

            Log.Debug("Executing query to build list of " + (primitive ? " primitives" : "objects") + " of type [" + objType.FullName.ToString() + "]");

            T newItem = default(T);
            List<T> listOfItems = new List<T>();

            Func<IDataReader, T> objBuilder = null;

            Log.Debug("Executing query...");

            DateTime queryStart = DateTime.Now;

            using (IDataReader reader = command.ExecuteReader())
            {
                Log.Debug("Query results took [" + DateTime.Now.Subtract(queryStart).TotalMilliseconds + "] ms");

                if (!primitive)
                    objBuilder = GetObjectBuilder<T>(command, reader);

                DateTime readerStart = DateTime.Now;

                while (reader.Read())
                {
                    if (primitive)
                        newItem = (T)Convert.ChangeType(reader.GetValue(0), objType);
                    else
                        newItem = objBuilder(reader);

                    listOfItems.Add(newItem);
                }
            }

            Log.Debug("Completed reading [" + listOfItems.Count + "] items in [" + DateTime.Now.Subtract(queryStart).TotalMilliseconds + "] ms");

            return listOfItems;
        }

        private static void SetParameter(IDbCommand command, string paramName, object obj)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = paramName;
            parameter.Value = obj;
            command.Parameters.Add(parameter);
        }

        private static object GetFieldFromReader(IDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;
            else
                return reader.GetValue(index);
        }
    }
}
