select

          ticket.id as ticket_id,
          ticket.created_at as ticket_created_at,
          ticket.description as ticket_description,
          ticket.due_at as ticket_due_at,
          ticket.has_incidents as ticket_has_incidents,
          ticket.is_public as ticket_is_public,
          ticket.priority as ticket_priority,
          ticket.recipient as ticket_recipient,
          ticket.status as ticket_status,
          ticket.subject as ticket_subject,
          ticket.type as ticket_type,
          ticket.via_channel as ticket_creation_channel,
          ticket.via_source_from_id as ticket_source_from_id,
          ticket.via_source_from_title as ticket_source_from_title,
          ticket.via_source_rel as ticket_source_rel,
          ticket.via_source_to_address as ticket_source_to_address,
          ticket.via_source_to_name as ticket_source_to_name,
          assignee.email as assignee_email,
          assignee.name as assignee_name,
          assignee.role as assignee_role,
          ticket_group.name as group_name,
          organization.name as organization_name,
          submitter.email as submitter_email,
          submitter.name as submitter_name,
          requester.email as requester_email,
          requester.name as requester_name,

      

      from `digital-arbor-400`.zendesk_new.ticket as ticket
      left join `digital-arbor-400`.zendesk_new.user as assignee on ticket.assignee_id = assignee.id -- need left join as not all tickets have assignees
      left join `digital-arbor-400`.zendesk_new.group as ticket_group on ticket.group_id = ticket_group.id -- cannot use keyword group so renamed; need left join
      left join `digital-arbor-400`.zendesk_new.organization as organization on ticket.organization_id = organization.id -- cannot use keyword group so renamed; need left join
      join `digital-arbor-400`.zendesk_new.user as requester on ticket.requester_id = requester.id
      join `digital-arbor-400`.zendesk_new.user as submitter on ticket.submitter_id = submitter.id