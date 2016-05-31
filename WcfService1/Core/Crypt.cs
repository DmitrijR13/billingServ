using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace ServiceFromBill.Core
{
    public static class Crypt
    {
        public static String GetHashStringMD5(string s)
        {
            //переводим строку в байт-массим  
            byte[] bytes = Encoding.ASCII.GetBytes(s);

            //создаем объект для получения средст шифрования  
            MD5CryptoServiceProvider CSP = new MD5CryptoServiceProvider();

            //вычисляем хеш-представление в байтах  
            byte[] byteHash = CSP.ComputeHash(bytes);

            string hash = string.Empty;

            //формируем одну цельную строку из массива  
            foreach (byte b in byteHash)
            {
                hash += string.Format("{0:x2}", b);
            }

            return hash;
        }
    }
}