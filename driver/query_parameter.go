package driver

import "github.com/goccy/go-zetasql/ast"

// countQueryParameters は、Nodeの子孫ノードに含まれるQuery Parameterの数をカウントして返す。
func countQueryParameters(node ast.StatementNode) int {
	count := 0
	_ = ast.Walk(node, func(n ast.Node) error {
		_, ok := n.(*ast.ParameterExprNode)
		if !ok {
			return nil
		}
		count++
		return nil
	})
	return count
}
