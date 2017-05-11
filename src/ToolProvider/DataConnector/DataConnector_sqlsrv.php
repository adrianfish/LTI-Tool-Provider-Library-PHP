<?php

namespace IMSGlobal\LTI\ToolProvider\DataConnector;

use IMSGlobal\LTI\ToolProvider;
use IMSGlobal\LTI\ToolProvider\ConsumerNonce;
use IMSGlobal\LTI\ToolProvider\Context;
use IMSGlobal\LTI\ToolProvider\ResourceLink;
use IMSGlobal\LTI\ToolProvider\ResourceLinkShareKey;
use IMSGlobal\LTI\ToolProvider\ToolConsumer;
use IMSGlobal\LTI\ToolProvider\User;

/**
 * Class to represent an LTI Data Connector for MS SQLServer
 * NB This class assumes that a MSSQL connection has already been opened to the appropriate schema
 *
 * @author  Adrian Fish <a.fish@lancaster.ac.uk>
 * @copyright  Lancaster University
 * @date  2016
 * @version 3.0.0
 * @license http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */
class DataConnector_sqlsrv extends DataConnector {

   /**
    * Load tool consumer object.
    *
    * @param ToolConsumer $consumer ToolConsumer object
    *
    * @return boolean True if the tool consumer object was successfully loaded
    */
    public function loadToolConsumer($consumer) {

        $ok = false;
        if (!empty($consumer->getRecordId())) {
            $test = $consumer->getRecordId();
            $sql = 'SELECT consumer_pk, name, consumer_key256, consumer_key, secret, lti_version, ' .
                           'consumer_name, consumer_version, consumer_guid, ' .
                           'profile, tool_proxy, settings, protected, enabled, ' .
                           'enable_from, enable_until, last_access, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' ' .
                           "WHERE consumer_pk = ?";
        } else {
            $key256 = DataConnector::getConsumerKey($consumer->getKey());
            $test = $key256;
            $sql = 'SELECT consumer_pk, name, consumer_key256, consumer_key, secret, lti_version, ' .
                           'consumer_name, consumer_version, consumer_guid, ' .
                           'profile, tool_proxy, settings, protected, enabled, ' .
                           'enable_from, enable_until, last_access, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' ' .
                           "WHERE consumer_key256 = ?";
        }
        if ($stmt = sqlsrv_query($this->db, $sql, [&$test])) {
            $ok = true;
            while ($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)) {
                if (empty($key256) || empty($row['consumer_key']) || ($consumer->getKey() === $row['consumer_key'])) {
                    $consumer->setRecordId(intval($row['consumer_pk']));
                    $consumer->name = $row['name'];
                    $consumer->setkey(empty($row['consumer_key']) ? $row['consumer_key256'] : $row['consumer_key']);
                    $consumer->secret = $row['secret'];
                    $consumer->ltiVersion = $row['lti_version'];
                    $consumer->consumerName = $row['consumer_name'];
                    $consumer->consumerVersion = $row['consumer_version'];
                    $consumer->consumerGuid = $row['consumer_guid'];
                    $consumer->profile = json_decode($row['profile']);
                    $consumer->toolProxy = $row['tool_proxy'];
                    $settings = unserialize($row['settings']);
                    if (!is_array($settings)) {
                        $settings = array();
                    }
                    $consumer->setSettings($settings);
                    $consumer->protected = (intval($row['protected']) === 1);
                    $consumer->enabled = (intval($row['enabled']) === 1);
                    $consumer->enableFrom = null;
                    if (!is_null($row['enable_from'])) {
                        $consumer->enableFrom = strtotime($row['enable_from']);
                    }
                    $consumer->enableUntil = null;
                    if (!is_null($row['enable_until'])) {
                        $consumer->enableUntil = strtotime($row['enable_until']);
                    }
                    $consumer->lastAccess = null;
                    if (!is_null($row['last_access'])) {
                        $consumer->lastAccess = strtotime($row['last_access']);
                    }
                    $consumer->created = strtotime($row['created']);
                    $consumer->updated = strtotime($row['updated']);
                    $ok = true;
                    break;
                }
            }
            sqlsrv_free_stmt($stmt);
        }

        return $ok;
    }

   /**
    * Save tool consumer object.
    *
    * @param ToolConsumer $consumer Consumer object
    *
    * @return boolean True if the tool consumer object was successfully saved
    */
    public function saveToolConsumer($consumer) {

        $id = $consumer->getRecordId();
        $key = $consumer->getKey();
        $key256 = DataConnector::getConsumerKey($key);
        if ($key === $key256) {
            $key = null;
        }
        $protected = ($consumer->protected) ? 1 : 0;
        $enabled = ($consumer->enabled)? 1 : 0;
        $profile = (!empty($consumer->profile)) ? json_encode($consumer->profile) : null;
        $settingsValue = serialize($consumer->getSettings());
        $time = time();
        $now = date("{$this->dateFormat} {$this->timeFormat}", $time);
        $from = null;
        if (!is_null($consumer->enableFrom)) {
            $from = date("{$this->dateFormat} {$this->timeFormat}", $consumer->enableFrom);
        }
        $until = null;
        if (!is_null($consumer->enableUntil)) {
            $until = date("{$this->dateFormat} {$this->timeFormat}", $consumer->enableUntil);
        }
        $last = null;
        if (!is_null($consumer->lastAccess)) {
            $last = date($this->dateFormat, $consumer->lastAccess);
        }
        if (empty($id)) {
            $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' (consumer_key256, consumer_key, name, ' .
                           'secret, lti_version, consumer_name, consumer_version, consumer_guid, profile, tool_proxy, settings, protected, enabled, ' .
                           'enable_from, enable_until, last_access, created, updated) ' .
                           'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

            $params = [&$key256, &$key, &$consumer->name, &$consumer->secret, &$consumer->ltiVersion,
                            &$consumer->consumerName, &$consumer->consumerVersion, &$consumer->consumerGuid,
                            &$profile, &$consumer->toolProxy, &$settingsValue,
                            &$protected, &$enabled, &$from, &$until, &$last,
                            &$now, &$now];
        } else {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' SET ' .
                           'consumer_key256 = ?, consumer_key = ?, ' .
                           'name = ?, secret= ?, lti_version = ?, consumer_name = ?, consumer_version = ?, consumer_guid = ?, ' .
                           'profile = ?, tool_proxy = ?, settings = ?, ' .
                           'protected = ?, enabled = ?, enable_from = ?, enable_until = ?, last_access = ?, updated = ? ' .
                           'WHERE consumer_pk = ?';
            $id = $consumer->getRecordId();

            $params = [&$key256, &$key, &$consumer->name, &$consumer->secret, &$consumer->ltiVersion,
                            &$consumer->consumerName, &$consumer->consumerVersion, &$consumer->consumerGuid,
                            &$profile, &$consumer->toolProxy, &$settingsValue,
                            &$protected, &$enabled, &$from, &$until, &$last,
                            &$now, &$id];
        }

        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $ok = true;
            if (empty($id)) {
                $consumer->setRecordId($this->sqlsrv_insert_id());
                $consumer->created = $time;
            }
            $consumer->updated = $time;
            sqlsrv_free_stmt($stmt);
        } else {
            $ok = false;
			$this->log_db_errors();
		}

        return $ok;
    }

   /**
    * Delete tool consumer object.
    *
    * @param ToolConsumer $consumer Consumer object
    * @return boolean True if the tool consumer object was successfully deleted
    */
    public function deleteToolConsumer($consumer) {

        $recordId = $consumer->getRecordId();

        // Delete any nonce values for this consumer
        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::NONCE_TABLE_NAME . ' WHERE consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any outstanding share keys for resource links for this consumer
        $sql = 'DELETE sk ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' sk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON sk.resource_link_pk = rl.resource_link_pk ' .
                       'WHERE rl.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any outstanding share keys for resource links for contexts in this consumer
        $sql = 'DELETE sk ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' sk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON sk.resource_link_pk = rl.resource_link_pk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ON rl.context_pk = c.context_pk ' .
                       'WHERE c.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any users in resource links for this consumer
        $sql = 'DELETE u ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' u ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON u.resource_link_pk = rl.resource_link_pk ' .
                       'WHERE rl.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any users in resource links for contexts in this consumer
        $sql = 'DELETE u ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' u ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON u.resource_link_pk = rl.resource_link_pk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ON rl.context_pk = c.context_pk ' .
                       'WHERE c.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Update any resource links for which this consumer is acting as a primary resource link
        $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' prl ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON prl.primary_resource_link_pk = rl.resource_link_pk ' .
                       'SET prl.primary_resource_link_pk = NULL, prl.share_approved = NULL ' .
                       'WHERE rl.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Update any resource links for contexts in which this consumer is acting as a primary resource link
        $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' prl ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON prl.primary_resource_link_pk = rl.resource_link_pk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ON rl.context_pk = c.context_pk ' .
                       'SET prl.primary_resource_link_pk = NULL, prl.share_approved = NULL ' .
                       'WHERE c.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any resource links for this consumer
        $sql = 'DELETE rl ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ' .
                       'WHERE rl.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any resource links for contexts in this consumer
        $sql = 'DELETE rl ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ON rl.context_pk = c.context_pk ' .
                       'WHERE c.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any contexts for this consumer
        $sql = 'DELETE c ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ' .
                       'WHERE c.consumer_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete consumer
        $sql = 'DELETE c ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' c ' .
                       'WHERE c.consumer_pk = ?';
        $ok = $this->execute_and_free($sql, [&$recordId]);

        if ($ok) {
            $consumer->initialize();
        }

        return $ok;
    }

    public function getToolConsumers() {

        $consumers = array();

        $sql = 'SELECT consumer_pk, consumer_key256, consumer_key, name, secret, lti_version, consumer_name, consumer_version, consumer_guid, ' .
               'profile, tool_proxy, settings, ' .
               'protected, enabled, enable_from, enable_until, last_access, created, updated ' .
               "FROM {$this->dbTableNamePrefix}" . DataConnector::CONSUMER_TABLE_NAME . ' ' .
               'ORDER BY name';
        if ($stmt = sqlsrv_query($this->db, $sql)) {
            while ($row = sqlsrv_fetch_object($stmt)) {
                $key = empty($row->consumer_key) ? $row->consumer_key256 : $row->consumer_key;
                $consumer = new ToolProvider\ToolConsumer($key, $this);
                $consumer->setRecordId(intval($row->consumer_pk));
                $consumer->name = $row->name;
                $consumer->secret = $row->secret;
                $consumer->ltiVersion = $row->lti_version;
                $consumer->consumerName = $row->consumer_name;
                $consumer->consumerVersion = $row->consumer_version;
                $consumer->consumerGuid = $row->consumer_guid;
                $consumer->profile = json_decode($row->profile);
                $consumer->toolProxy = $row->tool_proxy;
                $settings = unserialize($row->settings);
                if (!is_array($settings)) {
                    $settings = array();
                }
                $consumer->setSettings($settings);
                $consumer->protected = (intval($row->protected) === 1);
                $consumer->enabled = (intval($row->enabled) === 1);
                $consumer->enableFrom = null;
                if (!is_null($row->enable_from)) {
                    $consumer->enableFrom = strtotime($row->enable_from);
                }
                $consumer->enableUntil = null;
                if (!is_null($row->enable_until)) {
                    $consumer->enableUntil = strtotime($row->enable_until);
                }
                $consumer->lastAccess = null;
                if (!is_null($row->last_access)) {
                    $consumer->lastAccess = strtotime($row->last_access);
                }
                $consumer->created = strtotime($row->created);
                $consumer->updated = strtotime($row->updated);
                $consumers[] = $consumer;
            }
            sqlsrv_free_stmt($stmt);
        }

        return $consumers;
    }

   /**
    * Load the tool proxy from the database
    */
    public function loadToolProxy($toolProxy) {
        return false;
    }

   /**
    * Save the tool proxy to the database
    */
    public function saveToolProxy($toolProxy) {
        return false;
    }

   /**
    * Delete the tool proxy from the database
    */
    public function deleteToolProxy($toolProxy) {
        return false;
    }

   /**
    * Load context object.
    *
    * @param Context $context Context object
    * @return boolean True if the context object was successfully loaded
    */
    public function loadContext($context) {

        $ok = false;
        if (!empty($context->getRecordId())) {
            $sql = 'SELECT context_pk, consumer_pk, lti_context_id, settings, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' ' .
                           'WHERE (context_pk = ?)';
            $recordId = $context->getRecordId();
            $params = [&$recordId];
        } else {
            $sql = 'SELECT context_pk, consumer_pk, lti_context_id, settings, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' ' .
                           'WHERE (consumer_pk = ?) AND (lti_context_id = ?)';
            $recordId = $context->getConsumer()->getRecordId();
            $params = [&$recordId, &$context->ltiContextId];
        }
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $row = sqlsrv_fetch_object($stmt);
            if ($row) {
                $context->setRecordId(intval($row->context_pk));
                $context->setConsumerId(intval($row->consumer_pk));
                $context->ltiContextId = $row->lti_context_id;
                $settings = unserialize($row->settings);
                if (!is_array($settings)) {
                    $settings = array();
                }
                $context->setSettings($settings);
                $context->created = strtotime($row->created);
                $context->updated = strtotime($row->updated);
                $ok = true;
            }
            sqlsrv_free_stmt($stmt);
        }

        return $ok;
    }

   /**
    * Save context object.
    *
    * @param Context $context Context object
    * @return boolean True if the context object was successfully saved
    */
    public function saveContext($context) {

        $time = time();
        $now = date("{$this->dateFormat} {$this->timeFormat}", $time);
        $settingsValue = serialize($context->getSettings());
        $id = $context->getRecordId();
        $consumer_pk = $context->getConsumer()->getRecordId();
        $ltiContextId = $context->ltiContextId;
        if (empty($id)) {
            $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' (consumer_pk, lti_context_id, ' .
                           'settings, created, updated) ' .
                           'VALUES (?,?,?,?,?)';
            $params = [&$consumer_pk, &$ltiContextId, &$settingsValue, &$now, &$now];
        } else {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' SET ' .
                           'lti_context_id = ?, settings = ?, '.
                           'updated = ?' .
                           'WHERE (consumer_pk = ?) AND (context_pk = ?)';
            $params = [&$ltiContextId, &$settingsValue, &$now, &$consumer_pk, &$id];
        }
        $ok = false;
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $ok = true;
            if (empty($id)) {
                $context->setRecordId($this->sqlsrv_insert_id());
                $context->created = $time;
            }
            $context->updated = $time;
            sqlsrv_free_stmt($stmt);
            $ok = true;
        }

        return $ok;
    }

   /**
    * Delete context object.
    *
    * @param Context $context Context object
    *
    * @return boolean True if the Context object was successfully deleted
    */
    public function deleteContext($context) {

        $recordId = $context->getRecordId();

        // Delete any outstanding share keys for resource links for this context
        $sql = 'DELETE sk ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' sk ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON sk.resource_link_pk = rl.resource_link_pk ' .
                       'WHERE rl.context_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any users in resource links for this context
        $sql = 'DELETE u ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' u ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON u.resource_link_pk = rl.resource_link_pk ' .
                       'WHERE rl.context_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Update any resource links for which this consumer is acting as a primary resource link
        $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' prl ' .
                       "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ON prl.primary_resource_link_pk = rl.resource_link_pk ' .
                       'SET prl.primary_resource_link_pk = null, prl.share_approved = null ' .
                       'WHERE rl.context_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete any resource links for this consumer
        $sql = 'DELETE rl ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' rl ' .
                       'WHERE rl.context_pk = ?';
        $this->execute_and_free($sql, [&$recordId]);

        // Delete context
        $sql = 'DELETE c ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::CONTEXT_TABLE_NAME . ' c ' .
                       'WHERE c.context_pk = ?';
        $ok = $this->execute_and_free($sql, [&$recordId]);
        if ($ok) {
            $context->initialize();
        }

        return $ok;
    }

   /**
    * Load resource link object.
    *
    * @param ResourceLink $resourceLink Resource_Link object
    * @return boolean True if the resource link object was successfully loaded
    */
    public function loadResourceLink($resourceLink) {

        if (!empty($resourceLink->getRecordId())) {
            $sql = 'SELECT resource_link_pk, context_pk, consumer_pk, lti_resource_link_id, settings, primary_resource_link_pk, share_approved, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' ' .
                           'WHERE (resource_link_pk = ?)';
            $recordId = $resourceLink->getRecordId();
            $params = [&$recordId];
        } else if (!empty($resourceLink->getContext())) {
            $sql = 'SELECT resource_link_pk, context_pk, consumer_pk, lti_resource_link_id, settings, primary_resource_link_pk, share_approved, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' ' .
                           'WHERE (context_pk = %d) AND (lti_resource_link_id = %s)';
            $recordId = $resourceLink->getContext()->getRecordId();
            $resourceLinkId = $resourceLink->getId();
            $params = [&$recordId, &$resourceLinkId];
        } else {
            $sql = 'SELECT r.resource_link_pk, r.context_pk, r.consumer_pk, r.lti_resource_link_id, r.settings, r.primary_resource_link_pk, r.share_approved, r.created, r.updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' r LEFT OUTER JOIN ' .
                           $this->dbTableNamePrefix . DataConnector::CONTEXT_TABLE_NAME . ' c ON r.context_pk = c.context_pk ' .
                           ' WHERE ((r.consumer_pk = ?) OR (c.consumer_pk = ?)) AND (lti_resource_link_id = ?)';
            $consumerRecordId = $resourceLink->getConsumer()->getRecordId();
            $resourceLinkId = $resourceLink->getId();
            $params = [&$consumerRecordId, &$consumerRecordId, &$resourceLinkId];
        }

        $ok = false;
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $ok = true;
            $row = sqlsrv_fetch_object($stmt);
            if ($row) {
                $resourceLink->setRecordId(intval($row->resource_link_pk));
                if (!is_null($row->context_pk)) {
                    $resourceLink->setContextId(intval($row->context_pk));
                } else {
                    $resourceLink->setContextId(null);
                }
                if (!is_null($row->consumer_pk)) {
                    $resourceLink->setConsumerId(intval($row->consumer_pk));
                } else {
                    $resourceLink->setConsumerId(null);
                }
                $resourceLink->ltiResourceLinkId = $row->lti_resource_link_id;
                $settings = unserialize($row->settings);
                if (!is_array($settings)) {
                    $settings = array();
                }
                $resourceLink->setSettings($settings);
                if (!is_null($row->primary_resource_link_pk)) {
                    $resourceLink->primaryResourceLinkId = intval($row->primary_resource_link_pk);
                } else {
                    $resourceLink->primaryResourceLinkId = null;
                }
                $resourceLink->shareApproved = (is_null($row->share_approved)) ? null : (intval($row->share_approved) === 1);
                $resourceLink->created = strtotime($row->created);
                $resourceLink->updated = strtotime($row->updated);
                $ok = true;
            }
            sqlsrv_free_stmt($stmt);
        }

        return $ok;
    }

   /**
    * Save resource link object.
    *
    * @param ResourceLink $resourceLink Resource_Link object
    *
    * @return boolean True if the resource link object was successfully saved
    */
    public function saveResourceLink($resourceLink) {

        if (is_null($resourceLink->shareApproved)) {
            $approved = 'NULL';
        } else if ($resourceLink->shareApproved) {
            $approved = '1';
        } else {
            $approved = '0';
        }
        if (empty($resourceLink->primaryResourceLinkId)) {
            $primaryResourceLinkId = 'NULL';
        } else {
            $primaryResourceLinkId = strval($resourceLink->primaryResourceLinkId);
        }
        $time = time();
        $now = date("{$this->dateFormat} {$this->timeFormat}", $time);
        $settingsValue = serialize($resourceLink->getSettings());
        if (!empty($resourceLink->getContext())) {
            $consumerId = 'NULL';
            $contextId = strval($resourceLink->getContext()->getRecordId());
        } else if (!empty($resourceLink->getContextId())) {
            $consumerId = 'NULL';
            $contextId = strval($resourceLink->getContextId());
        } else {
            $consumerId = strval($resourceLink->getConsumer()->getRecordId());
            $contextId = 'NULL';
        }
        $id = $resourceLink->getRecordId();
        if (empty($id)) {
            $resourceLinkId = $resourceLink->getId();
            $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' (consumer_pk, context_pk, ' .
                           'lti_resource_link_id, settings, primary_resource_link_pk, share_approved, created, updated) ' .
                           'VALUES (?,?,?,?,?,?,?,?)';
            $params = [&$consumerId, &$contextId, &$resourceLinkId,
                            &$settingsValue, &$primaryResourceLinkId, &$approved, &$now, &$now];
        } else if ($contextId !== 'NULL') {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' SET ' .
                           'consumer_pk = ?, lti_resource_link_id = ?, settings = ?, '.
                           'primary_resource_link_pk = ?, share_approved = ?, updated = ? ' .
                           'WHERE (context_pk = ?) AND (resource_link_pk = ?)';
            $params = [&$consumerId, &$resourceLinkId, &$settingsValue, &$primaryResourceLinkId,
                            &$approved, &$now, &$contextId, &$id];
        } else {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' SET ' .
                           'context_pk = ?, lti_resource_link_id = ?, settings = ?, '.
                           'primary_resource_link_pk = ?, share_approved = ?, updated = ? ' .
                           'WHERE (consumer_pk = ?) AND (resource_link_pk = ?)';
            $params = [&$contextId, &$resourceLinkId, &$settingsValue, &$primaryResourceLinkId,
                            &$approved, &$now, &$consumerId, &$id];
        }
        $ok = false;
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $ok = true;
            if (empty($id)) {
                $resourceLink->setRecordId($this->sqlsrv_insert_id());
                $resourceLink->created = $time;
            }
            $resourceLink->updated = $time;
            sqlsrv_free_stmt($stmt);
            $ok = true;
        }

        return $ok;
    }

   /**
    * Delete resource link object.
    *
    * @param ResourceLink $resourceLink Resource_Link object
    *
    * @return boolean True if the resource link object was successfully deleted
    */
    public function deleteResourceLink($resourceLink) {

        $recordId = $resourceLink->getRecordId();
        // Delete any outstanding share keys for resource links for this consumer
        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' ' .
                       'WHERE (resource_link_pk = ?)';
        $ok = $this->execute_and_free($sql, [&$recordId]);

        // Delete users
        if ($ok) {
            $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' ' .
                'WHERE (resource_link_pk = ?)';
            $ok = $this->execute_and_free($sql, [&$recordId]);
        }

        // Update any resource links for which this is the primary resource link
        if ($ok) {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' ' .
                           'SET primary_resource_link_pk = NULL ' .
                           'WHERE (primary_resource_link_pk = ?)';
            $ok = $this->execute_and_free($sql, [&$recordId]);
        }

        // Delete resource link
        if ($ok) {
            $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' ' .
                           'WHERE (resource_link_pk = ?)';
            $ok = $this->execute_and_free($sql, [&$recordId]);
        }

        if ($ok) {
            $resourceLink->initialize();
        }

        return $ok;
    }

   /**
    * Get array of user objects.
    *
    * Obtain an array of User objects for users with a result sourcedId.  The array may include users from other
    * resource links which are sharing this resource link.  It may also be optionally indexed by the user ID of a specified scope.
    *
    * @param ResourceLink $resourceLink      Resource link object
    * @param boolean     $localOnly True if only users within the resource link are to be returned (excluding users sharing this resource link)
    * @param int         $idScope     Scope value to use for user IDs
    *
    * @return array Array of User objects
    */
    public function getUserResultSourcedIDsResourceLink($resourceLink, $localOnly, $idScope) {

        $users = array();

        $recordId = $resourceLink->getRecordId();
        if ($localOnly) {
            $sql = 'SELECT u.user_pk, u.lti_result_sourcedid, u.lti_user_id, u.created, u.updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' AS u '  .
                           "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' AS rl '  .
                           'ON u.resource_link_pk = rl.resource_link_pk ' .
                           "WHERE (rl.resource_link_pk = ?) AND (rl.primary_resource_link_pk IS NULL)";
            $params = [&$recordId];
        } else {
            $sql = 'SELECT u.user_pk, u.lti_result_sourcedid, u.lti_user_id, u.created, u.updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' AS u '  .
                           "INNER JOIN {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' AS rl '  .
                           'ON u.resource_link_pk = rl.resource_link_pk ' .
                           'WHERE ((rl.resource_link_pk = ?) AND (rl.primary_resource_link_pk IS NULL)) OR ' .
                           '((rl.primary_resource_link_pk = ?) AND (share_approved = 1))';
            $params = [&$recordId, &$recordId];
        }
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            while ($row = sqlsrv_fetch_object($stmt)) {
                $user = ToolProvider\User::fromResourceLink($resourceLink, $row->lti_user_id);
                $user->setRecordId(intval($row->user_pk));
                $user->ltiResultSourcedId = $row->lti_result_sourcedid;
                $user->created = strtotime($row->created);
                $user->updated = strtotime($row->updated);
                if (is_null($idScope)) {
                    $users[] = $user;
                } else {
                    $users[$user->getId($idScope)] = $user;
                }
            }
            sqlsrv_free_stmt($stmt);
        }

        return $users;
    }

   /**
    * Get array of shares defined for this resource link.
    *
    * @param ResourceLink $resourceLink Resource_Link object
    * @return array Array of ResourceLinkShare objects
    */
    public function getSharesResourceLink($resourceLink) {

        $shares = array();

        $sql = 'SELECT consumer_pk, resource_link_pk, share_approved ' .
                       "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_TABLE_NAME . ' ' .
                       'WHERE (primary_resource_link_pk = ?) ' .
                       'ORDER BY consumer_pk';
        $recordId = $resourceLink->getRecordId();
        if ($stmt = sqlsrv_query($this->db, $sql, [&$recordId])) {
            while ($row = sqlsrv_fetch_object($stmt)) {
                $share = new ToolProvider\ResourceLinkShare();
                $share->resourceLinkId = intval($row->resource_link_pk);
                $share->approved = (intval($row->share_approved) === 1);
                $shares[] = $share;
            }
            sqlsrv_free_stmt($stmt);
        }


        return $shares;
    }

   /**
    * Load nonce object.
    *
    * @param ConsumerNonce $nonce Nonce object
    * @return boolean True if the nonce object was successfully loaded
    */
    public function loadConsumerNonce($nonce) {

        $ok = true;

        // Delete any expired nonce values
        $now = date("{$this->dateFormat} {$this->timeFormat}", time());
        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::NONCE_TABLE_NAME . " WHERE expires <= ?";
        sqlsrv_query($this->db, $sql, [&$now]);

        // Load the nonce
        $sql = "SELECT value AS T FROM {$this->dbTableNamePrefix}" . DataConnector::NONCE_TABLE_NAME . ' WHERE consumer_pk = ? AND value = ?';
        $nonceRecordId = $nonce->getConsumer()->getRecordId();
        $nonceValue = $nonce->getValue();
        if ($stmt = sqlsrv_query($this->db, $sql, [&$nonceRecordId, &$nonceValue])) {
            error_log('nonce select executed');
            $ok = true;
            $row = sqlsrv_fetch_object($stmt);
            if ($row === false) {
                error_log('Failed to get nonce object from row');
                $ok = false;
            }
            sqlsrv_free_stmt($stmt);
        }

        return $ok;
    }

   /**
    * Save nonce object.
    *
    * @param ConsumerNonce $nonce Nonce object
    * @return boolean True if the nonce object was successfully saved
    */
    public function saveConsumerNonce($nonce) {

        $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::NONCE_TABLE_NAME . " (consumer_pk, value, expires) VALUES (?,?,?)";
        $consumerRecordId = $nonce->getConsumer()->getRecordId();
        $nonceValue = $nonce->getValue();
        $expires = date("{$this->dateFormat} {$this->timeFormat}", $nonce->expires);
        return $this->execute_and_free($sql, [&$consumerRecordId, &$nonceValue, &$expires]);
    }

   /**
    * Load resource link share key object.
    *
    * @param ResourceLinkShareKey $shareKey Resource_Link share key object
    * @return boolean True if the resource link share key object was successfully loaded
    */
    public function loadResourceLinkShareKey($shareKey) {

        $ok = false;

        // Clear expired share keys
        $now = date("{$this->dateFormat} {$this->timeFormat}", time());
        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . " WHERE expires <= ?";
        sqlsrv_free_stmt(sqlsrv_query($this->db, $sql, [&$now]));

        // Load share key
        $sql = 'SELECT resource_link_pk, auto_approve, expires ' .
               "FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' ' .
               "WHERE share_key_id = ?";
        $id = $shareKey->getId();
        if ($stmt = sqlsrv_query($this->db, $sql, [&$id])) {
            $row = sqlsrv_fetch_object($stmt);
            if ($row && (intval($row->resource_link_pk) === $shareKey->resourceLinkId)) {
                $shareKey->autoApprove = (intval($row->auto_approve) === 1);
                $shareKey->expires = strtotime($row->expires);
                $ok = true;
            }
            sqlsrv_free_stmt( $stmt);
        }

        return $ok;
    }

   /**
    * Save resource link share key object.
    *
    * @param ResourceLinkShareKey $shareKey Resource link share key object
    * @return boolean True if the resource link share key object was successfully saved
    */
    public function saveResourceLinkShareKey($shareKey) {

        if ($shareKey->autoApprove) {
            $approve = 1;
        } else {
            $approve = 0;
        }
        $expires = date("{$this->dateFormat} {$this->timeFormat}", $shareKey->expires);
        $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . ' ' .
                       '(share_key_id, resource_link_pk, auto_approve, expires) ' .
                       "VALUES (?,?,?,?)";
        $shareKeyId = $shareKey->getId();
        $resourceLinkId = $shareKey->resourceLinkId;
        return $this->execute_and_free($sql, [&$shareKeyId, &$resourceLinkId, &$approve, &$expires]);
    }

   /**
    * Delete resource link share key object.
    *
    * @param ResourceLinkShareKey $shareKey Resource link share key object
    * @return boolean True if the resource link share key object was successfully deleted
    */
    public function deleteResourceLinkShareKey($shareKey) {

        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::RESOURCE_LINK_SHARE_KEY_TABLE_NAME . " WHERE share_key_id = ?";
        $id = $shareKey->getId();

        $ok = $this->execute_and_free($sql, [&$id]);

        if ($ok) {
            $shareKey->initialize();
        }

        return $ok;
    }

   /**
    * Load user object.
    *
    * @param User $user User object
    * @return boolean True if the user object was successfully loaded
    */
    public function loadUser($user) {

        $ok = false;
        if (!empty($user->getRecordId())) {
            $sql = 'SELECT user_pk, resource_link_pk, lti_user_id, lti_result_sourcedid, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' ' .
                           'WHERE (user_pk = ?)';
            $recordId = $user->getRecordId();
            $params = [&$recordId];
        } else {
            $sql = 'SELECT user_pk, resource_link_pk, lti_user_id, lti_result_sourcedid, created, updated ' .
                           "FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' ' .
                           'WHERE (resource_link_pk = ?) AND (lti_user_id = ?)';
            $recordId = $user->getResourceLink()->getRecordId();
            $userId = $user->getId(ToolProvider\ToolProvider::ID_SCOPE_ID_ONLY);
            $params = [&$recordId, &$userId];
        }
        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            $row = sqlsrv_fetch_object($stmt);
            if ($row) {
                $user->setRecordId(intval($row->user_pk));
                $user->setResourceLinkId(intval($row->resource_link_pk));
                $user->ltiUserId = $row->lti_user_id;
                $user->ltiResultSourcedId = $row->lti_result_sourcedid;
                $user->created = strtotime($row->created);
                $user->updated = strtotime($row->updated);
                $ok = true;
            }
            sqlsrv_free_stmt($stmt);
        }

        return $ok;
    }

   /**
    * Save user object.
    *
    * @param User $user User object
    * @return boolean True if the user object was successfully saved
    */
    public function saveUser($user) {

        $time = time();
        $now = date("{$this->dateFormat} {$this->timeFormat}", $time);
        if (is_null($user->created)) {
            $sql = "INSERT INTO {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' (resource_link_pk, ' .
                           'lti_user_id, lti_result_sourcedid, created, updated) ' .
                           'VALUES (?,?,?,?,?)';
            $recordId = $user->getResourceLink()->getRecordId();
            $userId = $user->getId(ToolProvider\ToolProvider::ID_SCOPE_ID_ONLY);
            $sourceId = $user->ltiResultSourcedId;
            $params = [&$recordId, &$userId, &$sourceId, &$now, &$now];
        } else {
            $sql = "UPDATE {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' ' .
                           'SET lti_result_sourcedid = ?, updated = ? ' .
                           'WHERE (user_pk = ?)';
            $sourceId = $user->ltiResultSourcedId;
            $recordId = $user->getRecordId();
            $params = [&$sourceId, &$now, &$recordId];
        }
        $ok = $this->execute_and_free($sql, $params);
        if ($ok) {
            if (is_null($user->created)) {
                $user->setRecordId($this->sqlsrv_insert_id());
                $user->created = $time;
            }
            $user->updated = $time;
        }

        return $ok;
    }

   /**
    * Delete user object.
    *
    * @param User $user User object
    * @return boolean True if the user object was successfully deleted
    */
    public function deleteUser($user) {

        $sql = "DELETE FROM {$this->dbTableNamePrefix}" . DataConnector::USER_RESULT_TABLE_NAME . ' ' .
                       'WHERE (user_pk = ?)';
        $recordId = $user->getRecordId();
        $ok = $this->execute_and_free($sql, [&$recordId]);

        if ($ok) {
            $user->initialize();
        }

        return $ok;
    }

    private function execute_and_free($sql, $params) {

        if ($stmt = sqlsrv_query($this->db, $sql, $params)) {
            sqlsrv_free_stmt($stmt);
            return true;
        } else {
            return false;
        }
    }

    private function sqlsrv_insert_id() { 

        $id = 0; 
        if ($stmt = sqlsrv_query($this->db, "SELECT @@identity AS id")) {
            if ($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)) {
                $id = $row["id"];
            }
            sqlsrv_free_stmt($stmt);
        }
        return $id; 
    }

	private function log_db_errors() {

        $errors = sqlsrv_errors(SQLSRV_ERR_ALL);
        foreach ($errors as $error) {
			error_log("SQLSTATE: ".$error[ 'SQLSTATE']);
            error_log("code: ".$error[ 'code']);
            error_log("message: ".$error[ 'message']);
        }
	}
}
