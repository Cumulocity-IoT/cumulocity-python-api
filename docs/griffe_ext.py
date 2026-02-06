import griffe


class HideAttributes(griffe.Extension):
    def on_class(self, *, cls: griffe.Class, **kwargs) -> None:
        # Copy keys because we will delete while iterating
        for name, member in list(cls.members.items()):
            labels = getattr(member, "labels", set()) or set()
            if labels.intersection({"property", "class-attribute", "instance-attribute"}):
                cls.del_member(name)
