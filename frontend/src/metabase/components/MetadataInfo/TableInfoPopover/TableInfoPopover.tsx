import React from "react";
import PropTypes from "prop-types";
import { hideAll } from "tippy.js";

import TippyPopover, {
  ITippyPopoverProps,
} from "metabase/components/Popover/TippyPopover";

import { WidthBoundTableInfo } from "./TableInfoPopover.styled";

export const POPOVER_DELAY: [number, number] = [500, 300];

interface TableSubset {
  id: number | string;
  description?: string;
}

const propTypes = {
  table: PropTypes.shape({
    id: PropTypes.oneOf([PropTypes.number, PropTypes.string]),
    description: PropTypes.string,
  }).isRequired,
  children: PropTypes.node,
  placement: PropTypes.string,
  offset: PropTypes.arrayOf(PropTypes.number),
};

type Props = { table: TableSubset } & Pick<
  ITippyPopoverProps,
  "children" | "placement" | "offset"
>;

const className = "table-info-popover";

function TableInfoPopover({ table, children, placement, offset }: Props) {
  placement = placement || "left-start";

  const hasDescription = !!table.description;
  const isVirtualTable = typeof table.id === "string";
  const showPopover = hasDescription && !isVirtualTable;

  return showPopover ? (
    <TippyPopover
      className={className}
      interactive
      delay={POPOVER_DELAY}
      placement={placement}
      offset={offset}
      content={<WidthBoundTableInfo tableId={table.id} />}
      onTrigger={instance => {
        const dimensionInfoPopovers = document.querySelectorAll(
          `.${className}[data-state~='visible']`,
        );

        // if a dimension info popover is already visible, hide it and show this one immediately
        if (dimensionInfoPopovers.length > 0) {
          hideAll({
            exclude: instance,
          });
          instance.show();
        }
      }}
    >
      {children}
    </TippyPopover>
  ) : (
    children
  );
}

TableInfoPopover.propTypes = propTypes;

export default TableInfoPopover;
