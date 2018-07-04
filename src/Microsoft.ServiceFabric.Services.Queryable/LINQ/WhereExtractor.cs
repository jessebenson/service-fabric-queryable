using Microsoft.Data.OData.Query;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Microsoft.ServiceFabric.Services.Queryable.LINQ
{
    internal class WhereExtractor<TFilter> : ExpressionVisitor
    {
        public object Constant { get; private set; }
        public BinaryOperatorKind OperatorKind { get; private set; }
        public string PropertyName { get; private set; }
        private Expression expression;
        public WhereExtractor(Expression expression)
        {
            this.expression = expression;
            this.Visit(this.expression);
        }

        protected override Expression VisitBinary(BinaryExpression be)
        {
            // Right now this can only handle expressions one level deep (ie no ANDS, etc)
            // Can only product Expression Type Equal
            if (be.NodeType == ExpressionType.Equal)
            {
                MemberExpression member;
                ConstantExpression constant;
                if (be.Left is MemberExpression ^ be.Right is MemberExpression)
                {
                    if (be.Left is MemberExpression)
                    {
                        member = be.Left as MemberExpression;
                        constant = be.Right as ConstantExpression;
                    }
                    else
                    {
                        member = be.Right as MemberExpression;
                        constant = be.Left as ConstantExpression;
                    }
                }
                else
                {
                    return base.VisitBinary(be);
                }

                if (member.Member.DeclaringType != typeof(TFilter))
                {
                    throw new ArgumentException("Expression must have return type of TFilter");
                }

                OperatorKind = BinaryOperatorKind.Equal;
                Constant = constant.Value;
                PropertyName = member.Member.Name;
                return be;

            }
            else
                return base.VisitBinary(be);
        }

    }
}
