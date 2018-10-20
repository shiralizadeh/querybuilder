using System;

namespace SqlKata
{
    public class Include
    {
        public string Name { get; set; }
        public Query Query { get; set; }
        public string RelatedKey { get; set; }
        public string LocalKey { get; set; }
    }

    public class Include<T> : Include { };
}