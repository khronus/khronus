package com.despegar.metrik.web.service.influx.parser

trait Resolver extends Transformers with Traversals {
  case class ResolutionException(msg: String) extends RuntimeException(msg)

  def resolve(stmt: MetricCriteria, schema: Definitions): MetricCriteria = {
    // init contexts
    val n1 = topDownTransformationWithParent(stmt)((parent: Option[Node], child: Node) ⇒ child match {
      case s: MetricCriteria ⇒
        parent match {
          case _ ⇒
            (Some(s.copyWithContext(new Context(parent.map(c ⇒ Right(c.ctx)).getOrElse(Left(schema))))), true)
        }
      case e ⇒
        (Some(e.copyWithContext(parent.map(_.ctx).getOrElse(throw new RuntimeException("should have ctx")))), true)
    }).asInstanceOf[MetricCriteria]

    // build contexts up
    topDownTraversal(n1)(wrapReturnTrue {
      case s @ MetricCriteria(projections, relations, _, _, _, ctx) ⇒

        def checkName(name: String, alias: Option[String]): String = {
          alias.getOrElse(name)
        }

        def processTable(r: Table): Unit = r match {
          case Table(name, alias, _) ⇒

            // check name
            val name0 = checkName(name, alias)

            // check valid table
            val r = schema.defns.get(name).getOrElse(
              throw ResolutionException("no such table: " + name))

            // add relation to ctx
            ctx.relations += (name0 -> TableRelation(name))

        }

        // TODO - Fix malformed query without from clause
        processTable(relations.get)

        var seenWildcard = false
        projections.zipWithIndex.foreach {
          case (ExpressionProjection(f @ FieldIdent(qual, name, _, _), alias, _), idx) ⇒
            ctx.projections += NamedProjection(alias.getOrElse(name), f, idx)
          case (ExpressionProjection(expr, alias, _), idx) ⇒
            ctx.projections += NamedProjection(alias.getOrElse("$unknown$"), expr, idx)
          case (StarProjection(_), _) if !seenWildcard ⇒
            ctx.projections += WildcardProjection
            seenWildcard = true
          case _ ⇒
        }

      case _ ⇒ (None, true)
    })

    // resolve field idents
    def resolveFIs(ss: MetricCriteria): MetricCriteria = {
      def visit[N <: Node](n: N, allowProjs: Boolean): N = {
        topDownTransformation(n) {
          case f @ FieldIdent(qual, name, _, ctx) ⇒
            val cols = ctx.lookupColumn(qual, name, allowProjs)
            if (cols.isEmpty) throw new ResolutionException("no such column: " + f.sql)
            if (cols.size > 1) throw new ResolutionException("ambiguous reference: " + f.sql)
            (Some(FieldIdent(qual, name, cols.head, ctx)), false)
          case ss: MetricCriteria ⇒
            (Some(resolveFIs(ss)), false)
          case _ ⇒ (None, true)
        }.asInstanceOf[N]
      }
      val MetricCriteria(p, r, f, g, _, _) = ss
      ss.copy(
        projections = p.map(p0 ⇒ visit(p0, false)),
        relations = r.map(r0 ⇒ visit(r0, false)),
        filter = f.map(f0 ⇒ visit(f0, false)),
        groupBy = g.map(g0 ⇒ g0.copy(keys = g0.keys.map(k ⇒ visit(k, true)))))
    }

    val res = resolveFIs(n1)

    def fixContextProjections(ss: MetricCriteria): Unit = {
      val x = ss.ctx.projections.map {
        case n @ NamedProjection(_, _, pos) ⇒
          n.copy(expr = ss.projections(pos).asInstanceOf[ExpressionProjection].expr)
        case e ⇒ e
      }
      ss.ctx.projections.clear
      ss.ctx.projections ++= x
    }

    topDownTraversal(res)(wrapReturnTrue {
      case ss: MetricCriteria ⇒ fixContextProjections(ss)
      case _                  ⇒
    })
    res
  }
}
