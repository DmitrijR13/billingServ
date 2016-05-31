using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ServiceFromBill.Core
{
    public static class Configuration
    {
        /// <summary>
        /// Соль
        /// </summary>
        public static String Salt
        {
            get
            {
                return "huiktozalezet";
            }
        }

        /// <summary>
        /// Текущий день
        /// </summary>
        public static String CurrentDay
        {
            get
            {
                return DateTime.Now.ToString("dd");
            }
        }
    }
}