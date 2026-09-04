import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Transparent proxying',
    description: (
      <>
        Any standard Kafka client connects to kawa as if it were a broker.
        Requests are decoded on the wire, routed to the partition leader and
        mapped back — no client changes required.
      </>
    ),
  },
  {
    title: 'Virtual topics',
    description: (
      <>
        Clients use logical topic names that kawa rewrites to physical topics
        in both directions. Physical topics stay hidden in Metadata; consume
        filters drop records server-side with offsets preserved.
      </>
    ),
  },
  {
    title: 'Observable',
    description: (
      <>
        Built-in Micrometer metrics for requests, latency, bytes and virtual
        topic hits, exposed through an optional Prometheus text-format endpoint.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
