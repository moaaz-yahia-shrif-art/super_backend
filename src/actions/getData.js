module.exports = async ({ resource, id }, db) => {
  if (!db[resource]) throw new Error(`Resource [${resource}] not found`);

  if (id) {
    const item = db[resource].find((u) => u.id === parseInt(id));
    if (!item) throw new Error(`Item with ID [${id}] not found`);
    return item;
  }

  return db[resource];
};
