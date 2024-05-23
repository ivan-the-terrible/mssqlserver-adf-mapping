from anytree import Node, Resolver

test = Node("test", children=(Node("child1"), Node("child2")))

test.children += (Node("child1"),)
print(test.children)
# resolver = Resolver("name")
# resolver.get(test, "child1")
# print(resolver.get(test, "child1"))
