<?php
/**
 * Abstract class that define category attributes mapping
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade Smile Searchandising Suite to newer
 * versions in the future.
 *
 * This work is a fork of Johann Reinke <johann@bubblecode.net> previous module
 * available at https://github.com/jreinke/magento-elasticsearch
 *
 * @category  Smile
 * @package   Smile_ElasticSearch
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile
 * @license   Apache License Version 2.0
 */
class Smile_ElasticSearch_Model_Resource_Engine_Elasticsearch_Mapping_Category
    extends Smile_ElasticSearch_Model_Resource_Engine_Elasticsearch_Mapping_Catalog_Eav_Abstract
{
    /**
     * @var string
     */
    protected $_attributeCollectionModel = 'catalog/category_attribute_collection';

    /**
     * @var string
     */
    protected $_entityType = 'catalog_category';

    /**
     * @var Mage_Eav_Model_Entity_Attribute
     */
    protected $_isActiveAttribute;


    /**
     * Get mapping properties as stored into the index
     *
     * @return array
     */
    protected function _getMappingProperties()
    {
        $mapping = parent::_getMappingProperties(true);
        $mapping['properties']['path'] = array('type' => 'string');
        return $mapping;
    }

    /**
     * Retrive a bucket of indexable entities.
     *
     * @param int         $storeId Store id
     * @param string|null $ids     Ids filter
     * @param int         $lastId  First id
     *
     * @return array
     */
    protected function _getSearchableEntities($storeId, $ids = null, $lastId = 0)
    {
        $limit = $this->_getBatchIndexingSize();
        $rootCategoryId = Mage::app()->getStore($storeId)->getRootCategoryId();
        $rootCategory = Mage::getModel('catalog/category')->load($rootCategoryId);
        $rootPath = $rootCategory->getPath();

        $adapter   = $this->getConnection();

        $select = $adapter
            ->select()
            ->useStraightJoin(true)
            ->from(
                array('e' => $this->getTable('catalog/category'))
            );

        if (!is_null($ids)) {
            $select->where('e.entity_id IN(?)', $ids);
        }

        $select->where('e.entity_id>?', $lastId)
            ->where('path like ?', $rootPath . '/%')
            ->limit($limit)
            ->order('e.entity_id');

        // filter active categories
        $isActiveAttribute = $this->_getIsActiveAttribute();
        $isActiveValueTable = $isActiveAttribute->getBackendTable();
        $select->joinLeft($isActiveValueTable, 'e.entity_id = '.$isActiveValueTable.'.entity_id AND '.$isActiveValueTable.'.attribute_id = '.$isActiveAttribute->getId())
            ->where($isActiveValueTable.'.value = ?', 1);

        $result = array();
        $values = $adapter->fetchAll($select);
        foreach ($values as $value) {
            $result[$value['entity_id']] = $value;
        }

        return $result;
    }

    /**
     * Get is_active attribute for category entity
     *
     * @return Mage_Eav_Model_Entity_Attribute
     */
    protected function _getIsActiveAttribute()
    {
        if ($this->_isActiveAttribute == null) {
            $this->_isActiveAttribute = Mage::getModel('eav/entity_attribute')->loadByCode($this->_entityType, 'is_active');
        }
        return $this->_isActiveAttribute;
    }
}
