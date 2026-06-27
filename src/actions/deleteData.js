module.exports = async ({ resource, id }, db) => {
  if (!db[resource]) throw new Error(`Resource [${resource}] not found`);

  const index = db[resource].findIndex((u) => u.id === parseInt(id));
  if (index === -1) throw new Error(`Item with ID [${id}] not found`);

  const deletedItem = db[resource].splice(index, 1)[0];
  return { message: "Item deleted successfully", deletedItem };
};
