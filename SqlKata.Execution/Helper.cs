using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastMember;
using SqlKata;

namespace SqlKata.Execution
{
    internal static class QueryHelper
    {
        internal static MethodInfo GenericGetMethodInfo = typeof(QueryFactory).GetRuntimeMethods()
                                        .Where(x => x.Name == "Get" && x.GetGenericArguments().Any())
                                        .First();
        internal static XQuery CastToXQuery(Query query, string method = null)
        {
            var xQuery = query as XQuery;

            if (xQuery is null)
            {
                if (method == null)
                {
                    throw new InvalidOperationException($"Execution methods can only be used with `XQuery` instances, consider using the `QueryFactory.Query()` to create executable queries, check https://sqlkata.com/docs/execution/setup#xquery-class for more info");
                }
                else
                {
                    throw new InvalidOperationException($"The method ${method} can only be used with `XQuery` instances, consider using the `QueryFactory.Query()` to create executable queries, check https://sqlkata.com/docs/execution/setup#xquery-class for more info");
                }
            }

            return xQuery;

        }

        internal static QueryFactory CreateQueryFactory(XQuery xQuery)
        {
            var factory = new QueryFactory(xQuery.Connection, xQuery.Compiler);

            factory.Logger = xQuery.Logger;

            return factory;
        }

        internal static QueryFactory CreateQueryFactory(Query query)
        {
            var xQuery = CastToXQuery(query);

            var factory = new QueryFactory(xQuery.Connection, xQuery.Compiler);

            factory.Logger = xQuery.Logger;

            return factory;
        }

        internal static MethodInfo GetOfT(Type type)
        {
            return GenericGetMethodInfo.MakeGenericMethod(new[] { type });
        }

        internal static IList CreateListOfT(Type type)
        {
            Type generic = typeof(List<>).MakeGenericType(type);
            IList list = (IList)Activator.CreateInstance(generic);
            return list;
        }


        internal static void ResolveInclude<T>(QueryFactory db, Include include, List<T> rows)
        {

            if (rows.Count == 0)
            {
                return;
            }

            var parentAccessor = TypeAccessor.Create(rows[0].GetType());

            var children = GetChildren(db, parentAccessor, include, rows);

            if (children.Count == 0)
            {
                return;
            }

            var childType = children[0].GetType();
            var childAccessor = TypeAccessor.Create(children[0].GetType());
            var groupedChildren = (children as List<object>)
                .GroupBy(child => childAccessor[child, include.RelatedKey])
                .ToDictionary(x => (string)x.Key, x => x.ToList());

            foreach (var parent in rows)
            {
                var localKey = (string)parentAccessor[parent, include.LocalKey];
                var list = QueryHelper.CreateListOfT(childType);
                parentAccessor[parent, include.Name] = groupedChildren[localKey];
            }

        }

        internal static IList GetChildren<T>(QueryFactory db, TypeAccessor parentAccessor, Include include, IEnumerable<T> rows)
        {
            // if no query passed, we assume that the table name is the same as the property name
            var includeQuery = include.Query ?? new Query(include.Name);

            var ids = rows.Select(x => parentAccessor[x, include.LocalKey]).ToList();

            includeQuery.WhereIn(include.RelatedKey, ids);

            IList children;

            var childType = include.GetType().GenericTypeArguments.FirstOrDefault();

            if (childType == null)
            {
                children = db.Get(includeQuery).ToList();
            }
            else
            {
                children = (IList)QueryHelper.GetOfT(childType).Invoke(null, new object[] { db, includeQuery });
            }

            return children;
        }

    }
}