
import {Couchdb, nil} from './couchdb.js';

class BranchesOrder {

  constructor(abonents) {
    this.abonents = abonents;
  }

  /**
   * @summary Сортирует базы в порядке от детей к родителю
   * @param {Array.<String>} names
   * @param {Number} abonent
   * @param {Array.<Couchdb>} bases
   */
  sort(names, abonent, bases) {
    const dbs = names.filter(name => {
        if(name.startsWith(`wb_${abonent}`) && name.includes('_doc') && !bases[name]) {
          const parts = name.split('_');
          return !parts[3] || /^\d+$/.test(parts[3]);
        }
      })
      .map(name => {
        const root = name.endsWith('_doc');
        const branch = root ? null : this.branch(abonent, name);
        const order = root ? 1e6 : branch?.order || 0;
        return {name, branch: branch?.id || 0, order};
      });
    return dbs.sort((a, b) => a.order - b.order);
  }

  /**
   * Ищет отдел абонента по имени базы
   * @param {Number} abonentId
   * @param {String} name
   */
  branch(abonentId, name) {
    const abonent = this.abonents.find(v => v.id === abonentId);
    if(abonent) {
      const index = name.lastIndexOf('_');
      const suffix = name.substring(index + 1);
      return abonent.branches.find(v => v.suffix === suffix);
    }
  }

}

export async function branchesOrder() {
  const {DBUSER, DBPWD, COUCHLOCAL} = process.env;
  const db = new Couchdb(COUCHLOCAL, {auth: {username: DBUSER, password: DBPWD}});
  let res = await db.fetch('/wb_meta/_all_docs?start_key="cat.abonents|"&end_key="cat.abonents|z"&include_docs=true');
  const abonents = res.rows.map(({doc}) => {
    const {_id, id, name, server, area} = doc;
    const ref = _id.substring(13);
    return {ref, id, name, server, area, branches: []};
  }).filter(v => !v.area);

  res = await db.fetch('/wb_10_ram/_all_docs?start_key="cat.branches|"&end_key="cat.branches|z"&include_docs=true');
  const branches = res.rows.map(({doc}) => {
    const {_id, suffix, owner, parent, name, server} = doc;
    const ref = _id.substring(13);
    const abonent = abonents.find(v => v.ref === owner);
    const branch = {ref, id: parseInt(suffix), suffix, owner: abonent, parent, name, server, children: []};
    abonent?.branches.push(branch);
    return branch;
  });
  for(const branch of branches) {
    const {parent} = branch;
    const parentBrench = nil !== parent && branches.find(v => v.ref === parent);
    if(parentBrench) {
      branch.parent = parentBrench;
      parentBrench.children.push(branch);
    }
    else {
      branch.parent = {children: []};
    }
  }
  for(const branch of branches) {
    let {length} = branch.children;
    for(const sub of branch.children) {
      length += sub.children.length;
    }
    branch.order = length;
  }

  return new BranchesOrder(abonents);

}
const query = `
ВЫБРАТЬ
\tОтделы.Суффикс КАК Ссылка,
\tСУММА(ВЫБОР
\t\t\tКОГДА Дети.Ссылка ЕСТЬ NULL
\t\t\t\tТОГДА 0
\t\t\tИНАЧЕ 1
\t\tКОНЕЦ) КАК Детей
ИЗ
\tСправочник.ИнтеграцияОтделыАбонентов КАК Отделы
\t\tЛЕВОЕ СОЕДИНЕНИЕ Справочник.ИнтеграцияОтделыАбонентов КАК Дети
\t\tПО (Дети.Родитель = Отделы.Ссылка)
ГДЕ
\tОтделы.Владелец = &Абонент

СГРУППИРОВАТЬ ПО
\tОтделы.Суффикс

ИМЕЮЩИЕ
\tСУММА(ВЫБОР
\t\t\tКОГДА Дети.Ссылка ЕСТЬ NULL
\t\t\t\tТОГДА 0
\t\t\tИНАЧЕ 1
\t\tКОНЕЦ) > 0

УПОРЯДОЧИТЬ ПО
\tДетей УБЫВ`;
