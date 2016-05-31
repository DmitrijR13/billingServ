using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Oracle.DataAccess.Client;

//using System.Data.OracleClient;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            Connection();
        }

        private static DataTable Connection()
        {
            String connStr = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=194.84.250.9)(PORT=1521))(CONNECT_DATA=(SID=ezhkh)));User Id = mjf; Password = ActanonVerba";

            string cmdText = "SELECT NAME, ID FROM management_organization ORDER BY NAME";
            //string cmdText = "SELECT * FROM employees";
            OracleConnection conn = new OracleConnection(connStr);
            OracleCommand cmd = new OracleCommand(cmdText, conn);
            //cmd.Parameters.Add("surname", surname);
            OracleDataAdapter da = new OracleDataAdapter(cmd);
            //cmd.Parameters.Add("name", name);
            DataTable dt = new DataTable();
            //conn.Open();
            try
            {
                da.Fill(dt);
                return dt;
            }
            catch (Exception e)
            {
                string err = e.Message;
                return null;
            }
            finally
            {
                conn.Close();
            }

            
        }
    }
}
