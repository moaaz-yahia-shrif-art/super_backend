module.exports = async ({ resource, payload }, db) => {
  if (!db[resource]) throw new Error(`Resource [${resource}] not found`);
  if (!payload?.name) throw new Error("Invalid payload: name is required");

  const newItem = {
    id: db[resource].length + 1,
    ...payload,
  };

  db[resource].push(newItem);
  return newItem;
};
