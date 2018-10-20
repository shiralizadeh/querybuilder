using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using SqlKata;
using SqlKata.Compilers;

namespace SqlKata.Execution
{
    public class QueryFactory
    {
        public IDbConnection Connection { get; set; }
        public Compiler Compiler { get; set; }
        public Action<SqlResult> Logger = result => { };
        public int QueryTimeout { get; set; } = 30;

        public QueryFactory() { }

        public QueryFactory(IDbConnection connection, Compiler compiler)
        {
            Connection = connection;
            Compiler = compiler;
        }

        public Query Query()
        {
            var query = new XQuery(this.Connection, this.Compiler);

            query.Logger = Logger;

            return query;
        }

        public Query Query(string table)
        {
            return Query().From(table);
        }

        public Query FromQuery(Query query)
        {
            var xQuery = new XQuery(this.Connection, this.Compiler);

            xQuery.Clauses = query.Clauses.Select(x => x.Clone()).ToList();

            xQuery.QueryAlias = query.QueryAlias;
            xQuery.IsDistinct = query.IsDistinct;
            xQuery.Method = query.Method;

            xQuery.SetEngineScope(query.EngineScope);

            xQuery.Logger = Logger;

            return xQuery;
        }

        public IEnumerable<T> Get<T>(Query query)
        {
            var compiled = this.compile(query);

            var rows = this.Connection.Query<T>(
                compiled.Sql, compiled.NamedBindings
            ).ToList();

            if (query.Includes.Count == 0)
            {
                // no further processing return the result
                return rows;
            }

            foreach (var include in query.Includes)
            {
                QueryHelper.ResolveInclude(this, include, rows);
            }

            return rows;
        }

        public IEnumerable<IDictionary<string, object>> GetDictionary(Query query)
        {
            var compiled = this.compile(query);

            return this.Connection.Query(compiled.Sql, compiled.NamedBindings) as IEnumerable<IDictionary<string, object>>;
        }

        public IEnumerable<dynamic> Get(Query query)
        {
            return this.Get<dynamic>(query);
        }

        public T First<T>(Query query)
        {
            var compiled = this.compile(query.Limit(1));

            var row = this.Connection.QueryFirst<T>(compiled.Sql, compiled.NamedBindings);

            if (query.Includes.Count == 0)
            {
                // no further processing return the result
                return row;
            }

            foreach (var include in query.Includes)
            {
                QueryHelper.ResolveInclude(this, include, new[] { row }.ToList());
            }

            return row;

        }

        public dynamic First(Query query)
        {
            return First<dynamic>(query);
        }

        public T FirstOrDefault<T>(Query query)
        {
            var compiled = this.compile(query.Limit(1));

            var row = this.Connection.QueryFirstOrDefault<T>(compiled.Sql, compiled.NamedBindings);

            if (query.Includes.Count == 0)
            {
                // no further processing return the result
                return row;
            }

            foreach (var include in query.Includes)
            {
                QueryHelper.ResolveInclude(this, include, new[] { row }.ToList());
            }

            return row;

        }

        public dynamic FirstOrDefault(Query query)
        {
            return FirstOrDefault<dynamic>(query);
        }

        public int Execute(Query query, IDbTransaction transaction = null, CommandType? commandType = null)
        {
            var compiled = this.compile(query);

            return this.Connection.Execute(
                compiled.Sql,
                compiled.NamedBindings,
                transaction,
                this.QueryTimeout,
                commandType
            );
        }

        public T ExecuteScalar<T>(Query query, IDbTransaction transaction = null, CommandType? commandType = null)
        {
            var compiled = this.compile(query.Limit(1));

            return this.Connection.ExecuteScalar<T>(
                compiled.Sql,
                compiled.NamedBindings,
                transaction,
                this.QueryTimeout,
                commandType
            );
        }

        public SqlMapper.GridReader GetMultiple<T>(
            Query[] queries,
            IDbTransaction transaction = null,
            CommandType? commandType = null
        )
        {

            var compiled = queries
                .Select(q => this.compile(q))
                .Aggregate((a, b) => a + b);

            return this.Connection.QueryMultiple(
                compiled.Sql,
                compiled.NamedBindings,
                transaction,
                this.QueryTimeout,
                commandType
            );

        }

        public IEnumerable<IEnumerable<T>> Get<T>(
            Query[] queries,
            IDbTransaction transaction = null,
            CommandType? commandType = null
        )
        {
            var multi = this.GetMultiple<T>(
                queries,
                transaction,
                commandType
            );

            using (multi)
            {
                for (var i = 0; i < queries.Count(); i++)
                {
                    yield return multi.Read<T>();
                }
            }

        }

        public T Aggregate<T>(
                   Query query,
                   string aggregateOperation,
                   params string[] columns
               )
        {
            return this.ExecuteScalar<T>(query.AsAggregate(aggregateOperation, columns));
        }

        public T Count<T>(Query query, params string[] columns)
        {
            return this.ExecuteScalar<T>(query.AsCount(columns));
        }

        public T Average<T>(Query query, string column)
        {
            return this.Aggregate<T>(query, "avg", column);
        }

        public T Sum<T>(Query query, string column)
        {
            return this.Aggregate<T>(query, "sum", column);
        }

        public T Min<T>(Query query, string column)
        {
            return this.Aggregate<T>(query, "min", column);
        }

        public T Max<T>(Query query, string column)
        {
            return this.Aggregate<T>(query, "max", column);
        }

        private SqlResult compile(Query query)
        {
            var compiled = this.Compiler.Compile(query);

            this.Logger(compiled);

            return compiled;
        }

        public IEnumerable<T> Select<T>(string sql, object param = null)
        {
            return this.Connection.Query<T>(sql, param);
        }

        public IEnumerable<dynamic> Select(string sql, object param = null)
        {
            return this.Select<dynamic>(sql, param);
        }

        public int Statement(string sql, object param = null)
        {
            return this.Connection.Execute(sql, param);
        }

        public async Task<IEnumerable<T>> SelectAsync<T>(string sql, object param = null)
        {
            return await this.Connection.QueryAsync<T>(sql, param);
        }

        public async Task<IEnumerable<dynamic>> SelectAsync(string sql, object param = null)
        {
            return await this.SelectAsync<dynamic>(sql, param);
        }

        public async Task<int> StatementAsync(string sql, object param = null)
        {
            return await this.Connection.ExecuteAsync(sql, param);
        }

        public PaginationResult<T> Paginate<T>(Query query, int page, int perPage = 25)
        {

            if (page < 1)
            {
                throw new ArgumentException("Page param should be greater than or equal to 1", nameof(page));
            }

            if (perPage < 1)
            {
                throw new ArgumentException("PerPage param should be greater than or equal to 1", nameof(perPage));
            }

            var count = query.Clone().Count<long>();

            IEnumerable<T> list;
            if (count > 0)
            {
                list = query.Clone().ForPage(page, perPage).Get<T>();
            }
            else
            {
                list = Enumerable.Empty<T>();
            }

            return new PaginationResult<T>
            {
                Query = query,
                Page = page,
                PerPage = perPage,
                Count = count,
                List = list
            };

        }

        public void Chunk<T>(Query query, int chunkSize, Func<IEnumerable<T>, int, bool> func)
        {
            var result = this.Paginate<T>(query, 1, chunkSize);

            if (!func(result.List, 1))
            {
                return;
            }

            while (result.HasNext)
            {
                result = result.Next();
                if (!func(result.List, result.Page))
                {
                    return;
                }
            }

        }

        public void Chunk<T>(Query query, int chunkSize, Action<IEnumerable<T>, int> action)
        {
            var result = this.Paginate<T>(query, 1, chunkSize);

            action(result.List, 1);

            while (result.HasNext)
            {
                result = result.Next();
                action(result.List, result.Page);
            }

        }

    }
}