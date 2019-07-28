namespace SqlKata
{
    public class Expressions
    {
        /// <summary>
        /// Instruct the compiler to fetch the value from the predefined variables
        /// In the current query or parents queries.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Variable Variable(string name)
        {
            return new Variable(name);
        }

        /// <summary>
        /// Instruct the compiler to treat this as a literal.
        /// WARNING: don't pass user data directly to this method.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static UnsafeLiteral UnsafeLiteral(string value)
        {
            return new UnsafeLiteral(value);
        }
    }
}